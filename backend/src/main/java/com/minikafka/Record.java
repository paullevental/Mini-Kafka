package com.minikafka;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;


// class representing On-disk format for Record storage
public class Record {

    private final long offset;
    private final long timeStamp;
    // key used for hashing and partitioning
    private final byte[] key;
    private final byte[] value;

    public Record(long offset, long timeStamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timeStamp = timeStamp;
        this.key = key;
        this.value = value;
    }

    public byte[] serialize() {
        CRC32 crc32 = new CRC32();
        int valueLength = value.length;
        int keyLength = (key == null) ? -1 : key.length;
        int size = 32 + Math.max(0, keyLength) + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.position(8);
        buffer.putLong(offset);
        buffer.putLong(timeStamp);
        buffer.putInt(keyLength);
        if (key != null) {buffer.put(key);}
        buffer.putInt(valueLength);
        buffer.put(value);
        byte[] array = buffer.array();
        crc32.update(array, 8, array.length - 8);
        buffer.putInt(0, size - 4);
        buffer.putInt(4, (int) crc32.getValue());
        byte[] res = buffer.array();
        return res;
    }

    public static Record deserialize(ByteBuffer buffer) {
        byte[] bufferArray = buffer.array();
        int start = buffer.position();
        int length = buffer.getInt();
        int crc32Value = buffer.getInt();
        CRC32 crc32 = new CRC32();
        crc32.update(bufferArray, start + 8, length - 4);
        if (crc32Value != (int) crc32.getValue()) {
            throw new IllegalStateException("CRC32 Mismatch");
        }
        long offset = buffer.getLong();
        long timeStamp = buffer.getLong();
        int keyLength = buffer.getInt();
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            buffer.get(key);
        }
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);
        Record record = new Record(offset, timeStamp, key, value);

        return record;
    }

}
