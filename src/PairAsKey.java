import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairAsKey implements WritableComparable, Writable
{
    private Text w1;
    private Text w2;

    public PairAsKey(Text w1, Text w2)
    {
        this.w1 = w1;
        this.w2 = w2;
    }

    public PairAsKey()
    {
        this.w1 = new Text();
        this.w2 = new Text();
    }

    public Text getW2()
    {
        return w2;
    }

    public Text getW1()
    {
        return w1;
    }

    @Override
    public int compareTo(Object o)
    {
        PairAsKey other = (PairAsKey) o;
        Text other_w1 = other.w1;
        Text other_w2 = other.w2;

        // if w1 is different between this and other sort by w1
        if (this.w1.compareTo(other_w1) != 0)
        {
            return this.w1.compareTo(other_w1);
        }

        // if w1 is equal sort by w2 with priority to '*'
        if(this.w2.equals("*"))
            return -1;
        if(other_w2.equals("*"))
            return 1;
        return (this.w2.compareTo(other_w2));
    }

    @Override
    public String toString()
    {
        return "<" + w1.toString() + ", " + w2.toString() + ">";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(w1.toString());
        dataOutput.writeUTF(w2.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        w1 = new Text(dataInput.readUTF());
        w2 = new Text(dataInput.readUTF());
    }
}
