package com.github.yuanrw.im.connector.service;

import com.github.yuanrw.im.common.domain.ack.ServerAckWindow;
import com.github.yuanrw.im.common.domain.conn.Conn;
import com.github.yuanrw.im.common.domain.constant.MsgVersion;
import com.github.yuanrw.im.common.util.IdWorker;
import com.github.yuanrw.im.connector.domain.ClientConn;
import com.github.yuanrw.im.connector.domain.ClientConnContext;
import com.github.yuanrw.im.connector.handler.ConnectorTransferHandler;
import com.github.yuanrw.im.protobuf.generate.Ack;
import com.github.yuanrw.im.protobuf.generate.Chat;
import com.google.inject.Inject;
import com.google.protobuf.Message;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Function;

/**
 * process msg the connector received,
 * if send to client, need change msg id.
 * Date: 2019-04-08
 * Time: 21:05
 *
 * @author yrw
 */
public class ConnectorToClientService {
    private static Logger logger = LoggerFactory.getLogger(ConnectorToClientService.class);

    private ClientConnContext clientConnContext;

    @Inject
    public ConnectorToClientService(ClientConnContext clientConnContext) {
        this.clientConnContext = clientConnContext;
    }

    public void doChatToClientAndFlush(Chat.ChatMsg msg) {
        doChatToClientOrTransferAndFlush(msg);
    }

    public void doSendAckToClientAndFlush(Ack.AckMsg ackMsg) {
        doSendAckToClientOrTransferAndFlush(ackMsg);
    }

    public void doChatToClientOrTransferAndFlush(Chat.ChatMsg chat) {
        /**
         * 发送消息给destId:
         * 1.cid为connectionId，标识相应的session
         * 2.IdWorker.nextId(cid):计算对应connection的消息Id，对于destId所在的session: mid=lastMid+1
         */
        boolean onTheMachine = sendMsg(chat.getDestId(),
            cid -> Chat.ChatMsg.newBuilder().mergeFrom(chat).setId(IdWorker.nextId(cid)).build());

        //send ack to from id
        /**
         * 发送ACk消息给fromId
         */
        if (onTheMachine) {
            /**
             * 从本地缓存中获取userId对应的连接
             */
            ClientConn conn = clientConnContext.getConnByUserId(chat.getFromId());
            /**
             * 若连接为空，则发送消息至Transfer进行转发
             */
            if (conn == null) {
                ChannelHandlerContext ctx = ConnectorTransferHandler.getOneOfTransferCtx(System.currentTimeMillis());
                ctx.writeAndFlush(getDelivered(ctx.channel().attr(Conn.NET_ID).get(), chat));
            } else {
                //need wait for ack
                Ack.AckMsg delivered = getDelivered(conn.getNetId(), chat);
                ServerAckWindow.offer(conn.getNetId(), delivered.getId(), delivered, m -> conn.getCtx().writeAndFlush(m));
            }
        }
    }

    /**
     * 发送ACK消息
     * @param ackMsg
     */
    public void doSendAckToClientOrTransferAndFlush(Ack.AckMsg ackMsg) {
        sendMsg(ackMsg.getDestId(),
            cid -> Ack.AckMsg.newBuilder().mergeFrom(ackMsg).setId(IdWorker.nextId(cid)).build());
    }

    /**
     * 构造已投递消息
     * @param connectionId
     * @param msg
     * @return
     */
    private Ack.AckMsg getDelivered(Serializable connectionId, Chat.ChatMsg msg) {
        return Ack.AckMsg.newBuilder()
            .setId(IdWorker.nextId(connectionId))
            .setVersion(MsgVersion.V1.getVersion())
            .setFromId(msg.getDestId())
            .setDestId(msg.getFromId())
            .setDestType(msg.getDestType() == Chat.ChatMsg.DestType.SINGLE ? Ack.AckMsg.DestType.SINGLE : Ack.AckMsg.DestType.GROUP)
            .setCreateTime(System.currentTimeMillis())
            .setMsgType(Ack.AckMsg.MsgType.DELIVERED)
            .setAckMsgId(msg.getId())
            .build();
    }

    /**
     * 发送消息: Chat.ChatMsg/Ack.AckMsg
     * 1.若destId对应的连接不在本台服务器，则将消息发送至Transfer进行转发，返回false
     * 2.若destId对应的连接在本台服务器，则将消息直接发送至对应用户，返回true
     * @param destId
     * @param generateMsg
     * @return
     */
    private boolean sendMsg(String destId, Function<Serializable, Message> generateMsg) {
        /**
         * 从本地缓存中获取userId对应的连接
         */
        Conn conn = clientConnContext.getConnByUserId(destId);
        if (conn == null) {
            ChannelHandlerContext ctx = ConnectorTransferHandler.getOneOfTransferCtx(System.currentTimeMillis());
            ctx.writeAndFlush(generateMsg.apply(ctx.channel().attr(Conn.NET_ID).get()));
            return false;
        } else {
            //the user is connected to this machine
            //won 't save chat histories
            /**
             * 为Connector收到的Message生成新的mid，对于destId所在的session: mid=lastMid+1
             */
            Message message = generateMsg.apply(conn.getNetId());
            Long msgId = null;
            if (message instanceof Chat.ChatMsg) {
                Chat.ChatMsg chatMsg = (Chat.ChatMsg) message;
                msgId = chatMsg.getId();
            } else {
                Ack.AckMsg ackMsg = (Ack.AckMsg)message;
                msgId = ackMsg.getId();
            }
            ServerAckWindow.offer(conn.getNetId(), msgId, message, m -> conn.getCtx().writeAndFlush(m));
            return true;
        }
    }
}
