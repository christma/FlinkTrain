package transformation;

public class Student {
    private Integer ID;
    private String name;
    private Integer age;

    public Student() {
    }

    public Student(Integer ID, String name, Integer age) {
        this.ID = ID;
        this.name = name;
        this.age = age;
    }

    public Integer getID() {
        return ID;
    }

    public void setID(Integer ID) {
        this.ID = ID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Student{" +
                "ID=" + ID +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
