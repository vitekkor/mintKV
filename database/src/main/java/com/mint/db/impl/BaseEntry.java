package com.mint.db.impl;


import com.mint.db.Entry;

public record BaseEntry<D>(D key, D value) implements Entry<D> {
    @Override
    public String toString() {



        return STR."{ key=\{key}, value=\{value} }";
    }
}
