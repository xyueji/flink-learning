package com.xyueji.flink.core.model;


public class Student {

  private long id;
  private String name;
  private String password;
  private long age;

  public Student(long id, String name, String password, long age) {
    this.id = id;
    this.name = name;
    this.password = password;
    this.age = age;
  }

  @Override
  public String toString() {
    return "Student{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", password='" + password + '\'' +
            ", age=" + age +
            '}';
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }


  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }


  public long getAge() {
    return age;
  }

  public void setAge(long age) {
    this.age = age;
  }

}
