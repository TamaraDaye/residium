#![allow(unused)]


pub enum Operation {
    put(key, stream, metadata), 
    get(key),
    head(key),
}
