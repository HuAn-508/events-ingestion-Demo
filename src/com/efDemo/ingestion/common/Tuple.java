package com.efDemo.ingestion.common;

public class Tuple <A,B>{

    public final A key;
    public final B value;


    public Tuple(A key, B value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?> tuple = (Tuple<?, ?>) o;
        return (!key.equals(tuple.key)?false:value.equals(tuple.value));
    }

  /*  @Override
    public int hashCode() {
        return 31* key.hashCode()+value.hashCode();
    }


 */
}