# Steps:

### Step1 counts 3-grams:
<pre style="max-height: 450px;">
<u>Mapper Input:</u>

< danny went home> 1991 5 ...

< danny went home> 1992 4 ...

< danny went bowling> 1991 3

< danny went bowling> 1992 4

<u>Mapper Output</u>:

< danny went home> 5

< danny went home> 4

<u>Reducer Input</u>:

< danny went bowling> [3,4]

< danny went home> [5,4]

<u>Reducer Output</u>:

< danny went bowling> 7

< danny went home> 9

</pre>

### Step 2 counts 1-grams:
<pre style="max-height: 450px;">

Gets step 1's output

<u>Mapper Input:</u>

< danny went bowling> 7

< danny went home> 9

<u>Mapper Output</u>:

< * > 21

< danny > 7

< went > 7

< bowling > 7

< * > 27

< danny > 9

< went > 9

< home > 9

<u>Reducer Input</u>:

< * > [21,27]

< bowling > [7]

< danny > [7,9]

< home > [9]

< went > [7,9]

<u>Reducer Output</u>:

< * > 48

< bowling > 7

< danny > 16

< home > 9

< went > 16

</pre>

### step 3
<pre style="max-height: 450px;">
Gets step 1's output 

And loads the 1-gram table into memory

<u>Mapper Input:</u>

< danny went bowling > 7

< danny went home > 9

<u>Mapper Output</u>:

< danny went * > 7

< danny went > < danny went bowling > 7

< danny went * > 9

< danny went > < danny went home > 9

<u>Reducer Input</u>:

< danny went * > [7,9]

< danny went > < danny went bowling > 7

< danny went > < danny went home > 9

<u>Reducer Output</u>:

< danny went bowling>  &nbsp;&nbsp; k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>
 
< danny went home >  &nbsp;&nbsp;k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>

</pre>

### step 4
<pre style="max-height: 450px;">

Gets step 1's output 

And loads the 1-gram table into memory

<u>Mapper Input:</u>

< danny went bowling > 7

< danny went home > 9

<u>Mapper Output</u>:

< went bowling>  < * > 7

< went bowling > < danny went bowling > 7 

< went home > < * > 9

< went home > < danny went home > 9

<u>Reducer Input</u>:

< went bowling>  < * > 7

< went bowling > < danny went bowling > 7 

< went home > < * > 9

< went home > < danny went home > 9

<u>Reducer Output</u>:

< danny went bowling >  &nbsp; &nbsp; (1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>bowling</sub> / C<sub>0</sub>)

< danny went home >  &nbsp; &nbsp;(1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>home</sub> / C<sub>0</sub>)

</pre>


### step 5
<pre style="max-height: 450px;">
Sums step 3 and 4 

<u>Mapper Input:</u>

< danny went bowling >  &nbsp; &nbsp; (1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>bowling</sub> / C<sub>0</sub>)

< danny went home >  &nbsp; &nbsp;(1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>home</sub> / C<sub>0</sub>)

< danny went bowling>  &nbsp;&nbsp; k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>
 
< danny went home >  &nbsp;&nbsp;k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>

<u>Mapper Output</u>:(Identity)

< danny went bowling >  &nbsp; &nbsp; (1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>bowling</sub> / C<sub>0</sub>)

< danny went home >  &nbsp; &nbsp;(1-k<sub>3</sub>)*k<sub>2</sub>* (N<sub>2</sub> / C<sub>1(went</sub>))  + (1-k<sub>3</sub>)*(1-k<sub>2</sub>)*(N<sub>home</sub> / C<sub>0</sub>)

< danny went bowling>  &nbsp;&nbsp; k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>
 
< danny went home >  &nbsp;&nbsp;k<sub>3</sub> * <frac>N<sub>3</sub>/C<sub>2</sub>




<u>Reducer Input</u>:

< danny went bowling >  [ term1, term2+term3]

< danny went home >  [ term1, term2+term3]

<u>Reducer Output</u>:

< danny went bowling >  term1+term2+term3]

< danny went home >  term1+term2+term3

</pre>

### step 6
<pre style="max-height: 450px;">
Just sorting 
</pre>