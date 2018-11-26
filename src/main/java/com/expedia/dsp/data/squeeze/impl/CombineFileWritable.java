package com.expedia.dsp.data.squeeze.impl;

import lombok.Getter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Combine file writable for combined file input.
 *
 * @author Yashraj R. Sontakke
 */
public class CombineFileWritable implements WritableComparable {

    public long offset;
    @Getter
    public String fileName;

    public CombineFileWritable() {
        super();
    }

    public CombineFileWritable(long offset, String fileName) {
        super();
        this.offset = offset;
        this.fileName = fileName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    @Override
    public int compareTo(Object o) {
        CombineFileWritable that = (CombineFileWritable) o;

        int f = this.fileName.compareTo(that.fileName);
        if (f == 0) {
            return (int) Math.signum((double) (this.offset - that.offset));
        }
        return f;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CombineFileWritable)
            return this.compareTo(obj) == 0;
        return false;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + (fileName != null ? fileName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CombineFileWritable{" +
                "offset=" + offset +
                ", fileName='" + fileName + '\'' +
                '}';
    }
}
