import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Gram implements WritableComparable, Writable
{
    private Text tag;
    private Text w1;
    private Text w2;
    private Text w3;
    private static final String oneGram="one-gram";
    private static final String twoGram="two-gram";
    private static final String threeGram="three-gram";
    private Gram(Text tag,Text w1, Text w2, Text w3){
        this.tag = tag;
        this.w1 = w1;
        this.w2 = w2;
        this.w3 = w3;
    }

    public Gram(){
        tag = new Text("");
        w1 = new Text("");
        w2 = new Text("");
        w3 = new Text("");
    }
    public static Gram oneGram(Text w1){
        return new Gram(new Text(oneGram),w1, new Text(""), new Text(""));
    }

    public static Gram twoGram(Text w1,Text w2){
        return new Gram(new Text(twoGram),w1, w2, new Text(""));
    }

    public static Gram threeGram(Text w1,Text w2, Text w3){
        return new Gram(new Text("three-gram"),w1, w2, w3);
    }

    @Override
    public int compareTo(Object o)
    {
        Gram other = (Gram) o;
        if (w1.toString().equals(other.w1.toString()) && w2.toString().equals(other.w2.toString()) && w3.toString().equals(other.w3.toString()))
            return 0;

        // one-gram before the rest
        if (this.tag.toString().equals(oneGram) && !other.tag.toString().equals(oneGram))
            return -1;
        if (!this.tag.toString().equals(oneGram) && other.tag.toString().equals(oneGram))
            return 1;

        // two-gram before three gram
        if (this.tag.toString().equals(twoGram) && other.tag.toString().equals(threeGram))
            return -1;
        if (this.tag.toString().equals(threeGram) && other.tag.toString().equals(twoGram))
            return 1;

        // here this.tag == other.tag

        // this and other are 1-grams
        if (this.tag.toString().equals(oneGram))
            return this.w1.toString().compareTo(other.w1.toString());

        // this and other are 2-grams
        if(this.tag.toString().equals(twoGram))
            if (this.w1.toString().equals(other.w1.toString()))
                return this.w2.toString().compareTo(other.w2.toString());
            else
                return this.w1.toString().compareTo(other.w1.toString());

        // this and other are 3-grams
        // first sort by w1
        if (this.w1.toString().compareTo(other.w1.toString()) != 0)
            return this.w1.toString().compareTo(other.w1.toString());

        // then by w2
        if (this.w2.toString().compareTo(other.w2.toString()) != 0)
            return this.w2.toString().compareTo(other.w2.toString());

        // then by w3
        if (this.w3.toString().compareTo(other.w3.toString()) != 0)
            this.w3.toString().compareTo(other.w3.toString());

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(tag.toString());
        dataOutput.writeUTF(w1.toString());
        dataOutput.writeUTF(w2.toString());
        dataOutput.writeUTF(w3.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        tag = new Text(dataInput.readUTF());
        w1 = new Text(dataInput.readUTF());
        w2 = new Text(dataInput.readUTF());
        w3 = new Text(dataInput.readUTF());
    }

    @Override
    public String toString()
    {
        if(tag.toString().equals("one-gram"))
        {
            return w1.toString();
        }
        else if (tag.toString().equals("two-gram"))
        {
            return w1.toString() + ", " + w2.toString();
        }
        else {
            return w1.toString() + ", " + w2.toString() + ", " + w3.toString();
        }
    }
}


