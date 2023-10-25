package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityA 1..1<>----->0..n EntityB
 *          0..1<>----->1..1 EntityC <>----> EntityD
 *
 */
public class EntityA {

    private String id;   
    private String value;
    private String entityC;   
    
    
    public String getId() {
        return id;
    }

    public String getValue() {
        return value;
    }

    public String getEntityC() {
        return entityC;
    }
    
    public EntityA(String id, String value, String entityC) {
        super();
        this.id = id;
        this.value = value;
        this.entityC = entityC;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, value, entityC);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityA other = (EntityA) obj;
        return Objects.equals(id, other.id) && Objects.equals(value, other.value) && Objects.equals(entityC, other.entityC);
    }


    
}
