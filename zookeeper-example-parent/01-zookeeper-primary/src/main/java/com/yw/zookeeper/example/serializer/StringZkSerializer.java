package com.yw.zookeeper.example.serializer;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.Charset;

/**
 * @author yangwei
 */
public class StringZkSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return ((String) data).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes, Charset.forName("UTF-8"));
    }
}
