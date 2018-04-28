/*
 * This is the source code of tgnet library v. 1.0
 * It is licensed under GNU GPL v. 2 or later.
 * You should have received a copy of the license in this archive (see LICENSE).
 *
 * Copyright Nikolai Kudashov, 2015.
 */

#include <openssl/rand.h>
#include <stdlib.h>
#include "Connection.h"
#include "ConnectionsManager.h"
#include "BuffersStorage.h"
#include "FileLog.h"
#include "Timer.h"
#include "Datacenter.h"
#include "NativeByteBuffer.h"

static uint32_t lastConnectionToken = 1;

Connection::Connection(Datacenter *datacenter, ConnectionType type) {
    currentDatacenter = datacenter;
    connectionType = type;
    genereateNewSessionId();
    connectionState = TcpConnectionStageIdle;
    reconnectTimer = new Timer([&] {
        reconnectTimer->stop();
        connect();
    });
}

Connection::~Connection() {
    if (reconnectTimer != nullptr) {
        reconnectTimer->stop();
        delete reconnectTimer;
        reconnectTimer = nullptr;
    }
}

void Connection::suspendConnection() {
    suspendConnection(false);
}

void Connection::suspendConnection(bool idle) {
    reconnectTimer->stop();
    if (connectionState == TcpConnectionStageIdle || connectionState == TcpConnectionStageSuspended) {
        return;
    }
    DEBUG_D("connection(%p, dc%u, type %d) suspend", this, currentDatacenter->getDatacenterId(), connectionType);
    connectionState = idle ? TcpConnectionStageIdle : TcpConnectionStageSuspended;
    dropConnection();
    ConnectionsManager::getInstance().onConnectionClosed(this, 0);
    firstPacketSent = false;
    if (restOfTheData != nullptr) {
        restOfTheData->reuse();
        restOfTheData = nullptr;
    }
    lastPacketLength = 0;
    connectionToken = 0;
    wasConnected = false;
}

void Connection::onReceivedData(NativeByteBuffer *buffer) {
    failedConnectionCount = 0;

    NativeByteBuffer *parseLaterBuffer = nullptr;
    if (restOfTheData != nullptr) {
        if (lastPacketLength == 0) {
            if (restOfTheData->capacity() - restOfTheData->position() >= buffer->limit()) {
                restOfTheData->limit(restOfTheData->position() + buffer->limit());
                restOfTheData->writeBytes(buffer);
                buffer = restOfTheData;
            } else {
                NativeByteBuffer *newBuffer = BuffersStorage::getInstance().getFreeBuffer(restOfTheData->limit() + buffer->limit());
                restOfTheData->rewind();
                newBuffer->writeBytes(restOfTheData);
                newBuffer->writeBytes(buffer);
                buffer = newBuffer;
                restOfTheData->reuse();
                restOfTheData = newBuffer;
            }
        } else {
            uint32_t len;
            if (lastPacketLength - restOfTheData->position() <= buffer->limit()) {
                len = lastPacketLength - restOfTheData->position();
            } else {
                len = buffer->limit();
            }
            uint32_t oldLimit = buffer->limit();
            buffer->limit(len);
            restOfTheData->writeBytes(buffer);
            buffer->limit(oldLimit);
            if (restOfTheData->position() == lastPacketLength) {
                parseLaterBuffer = buffer->hasRemaining() ? buffer : nullptr;
                buffer = restOfTheData;
            } else {
                return;
            }
        }
    }

    buffer->rewind();

    while (buffer->hasRemaining()) {
        if (!hasSomeDataSinceLastConnect) {
            currentDatacenter->storeCurrentAddressAndPortNum();
            isTryingNextPort = false;
            if (connectionType == ConnectionTypePush) {
                setTimeout(60 * 15);
            } else {
                setTimeout(25);
            }
        }
        hasSomeDataSinceLastConnect = true;

        //TODO must delete
        buffer->rewind();

        uint32_t currentPacketLength = buffer->readUint32(nullptr)-12;
        //TODO must delete
        //if(currentPacketLength>4000)
            //return;
        uint32_t sequenceFromServer = buffer->readUint32(nullptr);


        uint32_t old = buffer->limit();
        buffer->limit(buffer->position() + currentPacketLength);
        ConnectionsManager::getInstance().onConnectionDataReceived(this, buffer, currentPacketLength);
        buffer->position(buffer->limit());
        buffer->limit(old);

        uint32_t checksum = buffer->readUint32(nullptr);

        if (restOfTheData != nullptr) {
            if ((lastPacketLength != 0 && restOfTheData->position() == lastPacketLength) || (lastPacketLength == 0 && !restOfTheData->hasRemaining())) {
                restOfTheData->reuse();
                restOfTheData = nullptr;
            } else {
                DEBUG_E("compact occured");
                restOfTheData->compact();
                restOfTheData->limit(restOfTheData->position());
                restOfTheData->position(0);
            }
        }

        if (parseLaterBuffer != nullptr) {
            buffer = parseLaterBuffer;
            parseLaterBuffer = nullptr;
        }
    }
}

void Connection::connect() {
    if (!ConnectionsManager::getInstance().isNetworkAvailable()) {
        ConnectionsManager::getInstance().onConnectionClosed(this, 0);
        return;
    }
    if ((connectionState == TcpConnectionStageConnected || connectionState == TcpConnectionStageConnecting)) {
        return;
    }
    connectionState = TcpConnectionStageConnecting;
    uint32_t ipv6 = ConnectionsManager::getInstance().isIpv6Enabled() ? TcpAddressFlagIpv6 : 0;
    uint32_t isStatic = !ConnectionsManager::getInstance().proxyAddress.empty() ? TcpAddressFlagStatic : 0;
    if (connectionType == ConnectionTypeDownload) {
        currentAddressFlags = TcpAddressFlagDownload | isStatic;
        hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags | ipv6);
        if (hostAddress.empty()) {
            currentAddressFlags = isStatic;
            hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags | ipv6);
        }
        if (hostAddress.empty() && ipv6) {
            ipv6 = 0;
            currentAddressFlags = TcpAddressFlagDownload | isStatic;
            hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags);
            if (hostAddress.empty()) {
                currentAddressFlags = isStatic;
                hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags);
            }
        }
    } else {
        currentAddressFlags = isStatic;
        hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags | ipv6);
        if (hostAddress.empty() && ipv6) {
            ipv6 = 0;
            hostAddress = currentDatacenter->getCurrentAddress(currentAddressFlags);
        }
    }
    hostPort = (uint16_t) currentDatacenter->getCurrentPort(currentAddressFlags);

    reconnectTimer->stop();

    DEBUG_D("connection(%p, dc%u, type %d) connecting (%s:%hu)", this, currentDatacenter->getDatacenterId(), connectionType, hostAddress.c_str(), hostPort);
    firstPacketSent = false;
    if (restOfTheData != nullptr) {
        restOfTheData->reuse();
        restOfTheData = nullptr;
    }
    lastPacketLength = 0;
    wasConnected = false;
    hasSomeDataSinceLastConnect = false;
    openConnection(hostAddress, hostPort, ipv6 != 0, ConnectionsManager::getInstance().currentNetworkType);
    if (connectionType == ConnectionTypePush) {
        if (isTryingNextPort) {
            setTimeout(20);
        } else {
            setTimeout(30);
        }
    } else {
        if (isTryingNextPort) {
            setTimeout(8);
        } else {
            if (connectionType == ConnectionTypeUpload) {
                setTimeout(25);
            } else {
                setTimeout(12);
            }
        }
    }
}

void Connection::reconnect() {
    forceNextPort = true;
    suspendConnection(true);
    connect();
}

bool Connection::hasUsefullData() {
    return usefullData;
}

void Connection::setHasUsefullData() {
    usefullData = true;
}

void Connection::sendData(NativeByteBuffer *buff, bool reportAck) {
    if (buff == nullptr) {
        return;
    }
    buff->rewind();
    if (connectionState == TcpConnectionStageIdle || connectionState == TcpConnectionStageReconnecting || connectionState == TcpConnectionStageSuspended) {
        connect();
    }

    if (isDisconnected()) {
        buff->reuse();
        DEBUG_D("connection(%p, dc%u, type %d) disconnected, don't send data", this, currentDatacenter->getDatacenterId(), connectionType);
        return;
    }

    //---no encryption!

    writeBuffer(buff);
}

void Connection::onDisconnected(int reason) {
    reconnectTimer->stop();
    DEBUG_D("connection(%p, dc%u, type %d) disconnected with reason %d", this, currentDatacenter->getDatacenterId(), connectionType, reason);
    bool switchToNextPort = wasConnected && !hasSomeDataSinceLastConnect && reason == 2 || forceNextPort;
    firstPacketSent = false;
    if (restOfTheData != nullptr) {
        restOfTheData->reuse();
        restOfTheData = nullptr;
    }
    connectionToken = 0;
    lastPacketLength = 0;
    wasConnected = false;
    if (connectionState != TcpConnectionStageSuspended && connectionState != TcpConnectionStageIdle) {
        connectionState = TcpConnectionStageIdle;
    }
    ConnectionsManager::getInstance().onConnectionClosed(this, reason);

    uint32_t datacenterId = currentDatacenter->getDatacenterId();
    if (connectionState == TcpConnectionStageIdle) {
        connectionState = TcpConnectionStageReconnecting;
        failedConnectionCount++;
        if (failedConnectionCount == 1) {
            if (usefullData) {
                willRetryConnectCount = 3;
            } else {
                willRetryConnectCount = 1;
            }
        }
        if (ConnectionsManager::getInstance().isNetworkAvailable()) {
            isTryingNextPort = true;
            if (failedConnectionCount > willRetryConnectCount || switchToNextPort) {
                currentDatacenter->nextAddressOrPort(currentAddressFlags);
                failedConnectionCount = 0;
            }
        }
        if (connectionType == ConnectionTypeGeneric && (currentDatacenter->isHandshaking() || datacenterId == ConnectionsManager::getInstance().currentDatacenterId || datacenterId == ConnectionsManager::getInstance().movingToDatacenterId)) {
            DEBUG_D("connection(%p, dc%u, type %d) reconnect %s:%hu", this, currentDatacenter->getDatacenterId(), connectionType, hostAddress.c_str(), hostPort);
            reconnectTimer->setTimeout(1000, false);
            reconnectTimer->start();
        }

    }
    usefullData = false;
}

void Connection::onConnected() {
    connectionState = TcpConnectionStageConnected;
    connectionToken = lastConnectionToken++;
    wasConnected = true;
    DEBUG_D("connection(%p, dc%u, type %d) connected to %s:%hu", this, currentDatacenter->getDatacenterId(), connectionType, hostAddress.c_str(), hostPort);
    ConnectionsManager::getInstance().onConnectionConnected(this);
}

Datacenter *Connection::getDatacenter() {
    return currentDatacenter;
}

ConnectionType Connection::getConnectionType() {
    return connectionType;
}

uint32_t Connection::getConnectionToken() {
    return connectionToken;
}
