package com.yrw.im.client.samples;

import com.yim.im.client.Client;
import com.yim.im.client.api.ChatApi;
import com.yim.im.client.api.ImClientApi;
import com.yim.im.client.api.UserApi;
import com.yrw.im.common.domain.UserInfo;
import com.yrw.im.common.domain.po.Relation;

import java.util.List;

/**
 * Date: 2019-05-15
 * Time: 13:57
 *
 * @author yrw
 */
public class ClientApplicationReceiver {

    public static void main(String[] args) {
        Client.start();
        UserApi userApi = ImClientApi.getApi(UserApi.class);
        ChatApi chatApi = ImClientApi.getApi(ChatApi.class);
        //登录换取token
        UserInfo user = userApi.login("xianyy", "123abc");

        //获取好友列表
//        List<Relation> friends = userApi.relations(user.getUserId(), user.getToken());
//        Relation relation = friends.get(0);

        //发送消息
//        chatApi.text(user.getUserId(),  getFriend(relation, user.getUserId()), "hello", user.getToken());
    }

    private static Long getFriend(Relation relation, Long userId) {
        return !relation.getUserId1().equals(userId) ? relation.getUserId1() : relation.getUserId2();
    }
}
