package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityA 1..1<>----->0..n EntityB
 *
 */
public class EntityB {

    private String id;   
    private String entityA;
    private String value;
    
    
    public String getId() {
        return id;
    }

    public String getEntityA() {
        return entityA;
    }
    
    public String getValue() {
        return value;
    }

    public EntityB(String id, String entityA, String value) {
        super();
        this.id = id;
        this.entityA = entityA;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityA, id, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityB other = (EntityB) obj;
        return Objects.equals(entityA, other.entityA) && Objects.equals(id, other.id) && Objects.equals(value, other.value);
    }

    
}
