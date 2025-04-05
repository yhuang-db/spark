package org.apache.spark.sql.catalyst.expressions;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.spark.sql.types.Decimal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import static org.apache.datasketches.common.ByteArrayUtil.copyBytes;
import static org.apache.datasketches.common.ByteArrayUtil.putIntLE;

public class ArrayOfDecimalByteArrSerDe extends ArrayOfItemsSerDe<Decimal> {
    private final int precision;
    private final int scale;

    public ArrayOfDecimalByteArrSerDe(int precision, int scale) {
        assert precision > Decimal.MAX_LONG_DIGITS();
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public byte[] serializeToByteArray(Decimal item) {
        Objects.requireNonNull(item, "Item must not be null");
        final byte[] decimalByteArr = item.toJavaBigDecimal().unscaledValue().toByteArray();
        final int numBytes = decimalByteArr.length;
        final byte[] out = new byte[numBytes + Integer.BYTES];
        copyBytes(decimalByteArr, 0, out, 4, numBytes);
        putIntLE(out, 0, numBytes);
        return out;
    }

    @Override
    public byte[] serializeToByteArray(Decimal[] items) {
        Objects.requireNonNull(items, "Items must not be null");
        if (items.length == 0) {
            return new byte[0];
        }
        int totalBytes = 0;
        final int numItems = items.length;
        final byte[][] serialized2DArray = new byte[numItems][];
        for (int i = 0; i < numItems; i++) {
            serialized2DArray[i] = items[i].toJavaBigDecimal().unscaledValue().toByteArray();
            totalBytes += serialized2DArray[i].length + Integer.BYTES;
        }
        final byte[] bytesOut = new byte[totalBytes];
        int offset = 0;
        for (int i = 0; i < numItems; i++) {
            final int decimalLen = serialized2DArray[i].length;
            putIntLE(bytesOut, offset, decimalLen);
            offset += Integer.BYTES;
            copyBytes(serialized2DArray[i], 0, bytesOut, offset, decimalLen);
            offset += decimalLen;
        }
        return bytesOut;
    }

    @Override
    public Decimal[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems) {
        Objects.requireNonNull(mem, "Memory must not be null");
        if (numItems <= 0) {
            return new Decimal[0];
        }
        final Decimal[] array = new Decimal[numItems];
        long offset = offsetBytes;
        for (int i = 0; i < numItems; i++) {
            Util.checkBounds(offset, Integer.BYTES, mem.getCapacity());
            final int decimalLength = mem.getInt(offset);
            offset += Integer.BYTES;
            final byte[] decimalBytes = new byte[decimalLength];
            Util.checkBounds(offset, decimalLength, mem.getCapacity());
            mem.getByteArray(offset, decimalBytes, 0, decimalLength);
            offset += decimalLength;
            BigInteger bigInteger = new BigInteger(decimalBytes);
            BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
            array[i] = Decimal.apply(javaDecimal, precision, scale);
        }
        return array;
    }

    @Override
    public int sizeOf(Decimal item) {
        Objects.requireNonNull(item, "Item must not be null");
        return item.toJavaBigDecimal().unscaledValue().toByteArray().length + Integer.BYTES;
    }

    @Override
    public int sizeOf(Memory mem, long offsetBytes, int numItems) {
        Objects.requireNonNull(mem, "Memory must not be null");
        if (numItems <= 0) {
            return 0;
        }
        long offset = offsetBytes;
        final long memCap = mem.getCapacity();
        for (int i = 0; i < numItems; i++) {
            Util.checkBounds(offset, Integer.BYTES, memCap);
            final int itemLenBytes = mem.getInt(offset);
            offset += Integer.BYTES;
            Util.checkBounds(offset, itemLenBytes, memCap);
            offset += itemLenBytes;
        }
        return (int) (offset - offsetBytes);
    }

    @Override
    public String toString(Decimal item) {
        if (item == null) {
            return "null";
        }
        return item.toString();
    }

    @Override
    public Class<Decimal> getClassOfT() {
        return Decimal.class;
    }
}
