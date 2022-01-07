import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombinedKey implements WritableComparable, Writable
{
    private Text key1;
    private Text key2;
    private final Gram gram;

    public CombinedKey(){
        key1 = new Text("");
        key2 = new Text("");
        gram = new Gram();
    }

    public CombinedKey(String key1, String key2, Gram gram){
        this.key1 = new Text(key1);
        this.key2 = new Text(key2);
        this.gram = gram;
    }

    @Override
    public int compareTo(Object o)
    {
        CombinedKey other = (CombinedKey) o;

        if(Gram.twoGram(key1,key2).compareTo(Gram.twoGram(other.key1, other.key2)) != 0)
            return Gram.twoGram(key1,key2).compareTo(Gram.twoGram(other.key1, other.key2));

        return this.gram.compareTo(other.gram);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(key1.toString());
        dataOutput.writeUTF(key2.toString());
        gram.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        key1 = new Text(dataInput.readUTF());
        key2 = new Text(dataInput.readUTF());
        gram.readFields(dataInput);
    }

    @Override
    public String toString()
    {
        return this.key1.toString() + " " + this.key2.toString() + " " + this.gram;
    }

    public Text getKey1()
    {
        return key1;
    }

    public Text getKey2()
    {
        return key2;
    }

    public Gram getGram()
    {
        return gram;
    }
}
