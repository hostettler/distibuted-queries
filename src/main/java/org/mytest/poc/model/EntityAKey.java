package org.mytest.poc.model;

import java.util.Objects;

public class EntityAKey {

    private String id;

    public EntityAKey(String id) {
        super();
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hash(id);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EntityAKey other = (EntityAKey) obj;
        return Objects.equals(id, other.id);
    }
    
}