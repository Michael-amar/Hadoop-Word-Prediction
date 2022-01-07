import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// in order to sort by the values we need to use the Value2Key technique
// this class is for that purpose
public class Custom3Gram implements WritableComparable, Writable
{
    private final Gram three_gram;
    private DoubleWritable value;

    public Custom3Gram(Gram three_gram, DoubleWritable value){
        this.three_gram = three_gram;
        this.value = value;
    }

    public Custom3Gram(){
        this.three_gram = new Gram();
        this.value = new DoubleWritable();
    }

    @Override
    public int compareTo(Object o)
    {
        Custom3Gram other = (Custom3Gram) o;

        Gram this_w1w2 = Gram.twoGram(three_gram.getW1(), three_gram.getW2());
        Gram other_w1w2 = Gram.twoGram(other.three_gram.getW1(), other.three_gram.getW2());

        if (this_w1w2.compareTo(other_w1w2) == 0)
            return -1 * Double.compare(this.value.get(), other.value.get());
        else
            return this_w1w2.compareTo(other_w1w2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        three_gram.write(dataOutput);
        dataOutput.writeDouble(value.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        three_gram.readFields(dataInput);
        value = new DoubleWritable(dataInput.readDouble());
    }

    public Gram getThree_gram()
    {
        return three_gram;
    }

    public DoubleWritable getValue()
    {
        return value;
    }
}
