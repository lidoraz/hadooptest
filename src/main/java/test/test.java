package test;

public class test {
    public static void main(String[] args) throws InterruptedException {
        String[] arr = "\"! ] ".split(" ");

//        double N1=94; // w3
//        double N2=94; // w2w3
//        double N3=94; // w1w2w3
//        double C2=162;  //w1w2
//        double C1=162; //w2 occr
//        double C0=531; //all words
//        double k2 = (Math.log10(N2 + 1) + 1) / (Math.log10(N2 + 1) + 2);
//        double k3 = (Math.log10(N3 + 1) + 1) / (Math.log10(N3 + 1) + 2);
//        double prob =(k3 * (N3 / C2) + (1 - k3) * k2 * (N2 / C1) + (1 - k3) * (1 - k2) * (N1 / C0));
//        System.out.println(prob);
        String gram3="אני סופר .";
        String gram3_3="אני סופר .";

        System.out.println((gram3.hashCode() & Integer.MAX_VALUE));
        System.out.println((gram3_3.hashCode() & Integer.MAX_VALUE));

//
//    }
//
    }
    public static void cesar(String str,int num){
        String s= "ABCabc";

        int shift=-1;
        char[] output=s.toCharArray();
        for(int i=0;i<s.length();i++){
            char key=output[i];
            if('A'<=key&&key<='Z'){
                char c= (char) (key-'A');
                c= (c<'A')? (char) (c + 26) :c;
                output[i]=((char)(((c+shift)%26)+'A'));
            }
            if('a'<=key&&key<='z') {
                char c= (char) (key-'a');
                c= (c<'a')? (char) (c + 26) :c;
                output[i] = (char)(((c + shift) % 26) + 'a');
            }

       //     output[i]=charat;


        }

        System.out.println(String.valueOf(output));
        //String.valueOf(output);
    }


}
