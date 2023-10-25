package org.mytest.poc.model;

import java.util.Objects;


/**
 * 
 *  EntityA 1..1<>----->0..n EntityB
 *
 */
public class EntityBKey {

    private String id;
    private String entityA;
    
    
    public String getId() {
        return id;
    }

    public String getEntityA() {
        return entityA;
    }
    

    public EntityBKey(String id, String entityA) {
        super();
        this.id = id;
        this.entityA = entityA;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityA, id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityBKey other = (EntityBKey) obj;
        return Objects.equals(entityA, other.entityA) && Objects.equals(id, other.id);
    }

    
}
