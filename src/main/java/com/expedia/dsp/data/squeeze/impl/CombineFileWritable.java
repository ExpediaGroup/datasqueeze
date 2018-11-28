/**
 * Copyright (C) 2018 Expedia Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
