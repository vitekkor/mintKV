package com.mint.db.dao;

public interface Entry<D> {
    D key();

    D committedValue();

    D uncommittedValue();
}
