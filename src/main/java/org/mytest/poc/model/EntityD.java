package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityC 1..1<>----->0..n EntityD
 *
 */
public class EntityD {

    private String id;   
    private String entityC;
    private String value;
    
    
    public String getId() {
        return id;
    }

    public String getEntityC() {
        return entityC;
    }
    
    public String getValue() {
        return value;
    }

    public EntityD(String id, String entityC, String value) {
        super();
        this.id = id;
        this.entityC = entityC;
        this.value = value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityC, id, value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityD other = (EntityD) obj;
        return Objects.equals(entityC, other.entityC) && Objects.equals(id, other.id) && Objects.equals(value, other.value);
    }

    
}
