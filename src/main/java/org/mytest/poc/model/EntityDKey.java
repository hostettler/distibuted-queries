package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityA 1..1<>----->0..n EntityB
 *
 */
public class EntityDKey {

    private String id;
    private String entityC;
    
    
    public String getId() {
        return id;
    }

    public String getEntityC() {
        return entityC;
    }
    

    public EntityDKey(String id, String entityC) {
        super();
        this.id = id;
        this.entityC = entityC;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityC, id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityDKey other = (EntityDKey) obj;
        return Objects.equals(entityC, other.entityC) && Objects.equals(id, other.id);
    }

    
}
