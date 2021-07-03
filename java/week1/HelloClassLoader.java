package week1;

import java.lang.reflect.Method;
import java.util.Base64;

public class HelloClassLoader extends ClassLoader{

        public static void main(String[] args) throws Exception {
            Class hello = new HelloClassLoader().findClass("Hello");
            Object obj = hello.newInstance();
            Method helloLoader = hello.getMethod("hello");
            helloLoader.invoke(obj);
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            String file="yv66vgAAADQAHAoABgAOCQAPABAIABEKABIAEwcAFAcAFQEABjxpbml0PgEAAygpVgEABENvZGUBAA9MaW5lTnVtYmVyVGFibGUBAAVoZWxsbwEAClNvdXJjZUZpbGUBAApIZWxsby5qYXZhDAAHAAgHABYMABcAGAEAE0hlbGxvLCBjbGFzc0xvYWRlciEHABkMABoAGwEABUhlbGxvAQAQamF2YS9sYW5nL09iamVjdAEAEGphdmEvbGFuZy9TeXN0ZW0BAANvdXQBABVMamF2YS9pby9QcmludFN0cmVhbTsBABNqYXZhL2lvL1ByaW50U3RyZWFtAQAHcHJpbnRsbgEAFShMamF2YS9sYW5nL1N0cmluZzspVgAhAAUABgAAAAAAAgABAAcACAABAAkAAAAdAAEAAQAAAAUqtwABsQAAAAEACgAAAAYAAQAAAAEAAQALAAgAAQAJAAAAJQACAAEAAAAJsgACEgO2AASxAAAAAQAKAAAACgACAAAABAAIAAUAAQAMAAAAAgAN";
            byte[] bytes = decode(file);
            return defineClass(name,bytes,0,bytes.length);
        }

        public byte[] decode(String code){
            return Base64.getDecoder().decode(code);
        }
}
