package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityC 1..1<>----->0..n EntityD
 *
 */
public class EntityC {

    private String id;   
    private String value;
    
    
    public String getId() {
        return id;
    }

    public String getValue() {
        return value;
    }


    
    public EntityC(String id, String value) {
        super();
        this.id = id;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value);
    }



    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityC other = (EntityC) obj;
        return Objects.equals(id, other.id) && Objects.equals(value, other.value);
    }


    
}
