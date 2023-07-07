package dev.phoenixtype.debeziumdemo.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

//import javax.persistence.Entity;
//import javax.persistence.Id;

@Entity
@Data
public class Customer {

    @Id
    private Long id;
    private String fullname;
    private String email;

}