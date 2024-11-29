import java.io.IOException;
import java.io.RandomAccessFile;

public class VarInt {
    long value;
    int sizeInBytes;

     VarInt(int sizeInBytes, long value) {
        this.sizeInBytes = sizeInBytes;
        this.value = value;
    }

    public static VarInt readVarInt(RandomAccessFile dbFile) throws IOException {
        long value = 0;
        int numBytes = 0;

        for (int i = 0; i < 9; i++) {
            int b = dbFile.readUnsignedByte();
            value = (value << 7) | (b & 0x7F);  // Mask out the MSB and shift in 7 bits

            numBytes++;
            if ((b & 0x80) == 0) {
                break;  // VarInt ends when the MSB is not set
            }
        }

        return new VarInt(numBytes, value);
    }

}
