package com.github.simplesteph.udemy.java.datagen;


import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Dataset {

    public static Schema UserSchema = SchemaBuilder.record("User")
            .namespace("com.github.simplesteph")
            .fields()
            .name("login").type().stringType().noDefault()
            .name("first-name").type().stringType().noDefault()
            .name("last-name").type().nullable().stringType().noDefault()
            .name("email").type().nullable().stringType().noDefault()
            .endRecord();

    public static Schema UserKeySchema = SchemaBuilder.record("UserKey")
            .namespace("com.github.simplesteph")
            .fields()
            .name("login").type().stringType().noDefault()
            .endRecord();

    public static Schema PurchaseSchema = SchemaBuilder.record("Purchase")
            .namespace("com.github.simplesteph")
            .fields()
            .name("user").type().stringType().noDefault()
            .name("game").type().enumeration("Game").namespace("com.github.simplesteph")
            .symbols("", "", "").noDefault()
            .name("two-player").type().booleanType().booleanDefault(false)
            .endRecord();

    public static Schema PurchaseKeySchema = SchemaBuilder.record("User")
            .namespace("com.github.simplesteph")
            .fields()
            .name("id").type().stringType().noDefault()
            .name("client").type((UserSchema)).noDefault()
            .endRecord();

}
