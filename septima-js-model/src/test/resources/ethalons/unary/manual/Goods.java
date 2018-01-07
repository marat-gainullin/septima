import com.septima.entities.SqlEntities;

import com.septima.model.Model;

import java.util.*;

public class Goods extends Model {

    public class Hierarchy extends GoodsHierarchyRow {

        public Hierarchy getParent() {
            if (getParentId() != null) {
                return Optional.ofNullable(hierarchy.getByKey().get(getParentId()))
                        .orElseThrow(() -> new IllegalStateException("Unresolved reference '" + hierarchy.getName() + "' (" + getParentId() + ")' in entity '" + hierarchy.getName() + "' (" + getId() + ")'"));
            } else {
                return null;
            }
        }

        public void setParent(Hierarchy aHierarchy) {
            Hierarchy old = getParent();
            if (old != null) {
                old.getChildren().remove(this);
            }
            setParentId(aHierarchy != null ? aHierarchy.getId() : null);
            if (aHierarchy != null) {
                aHierarchy.getChildren().add(this);
            }
        }

        public Collection<Hierarchy> getChildren() {
            return hierarchyByParentId.getOrDefault(getId(), List.of());
        }

    }

    private Map<Long, Collection<Hierarchy>> hierarchyByParentId = new HashMap<>();

    private final Entity<Long, Hierarchy> hierarchy = new Entity<>(
            "goods-hierarchy",
            "id",
            Hierarchy::getId,
            datum -> {
                Hierarchy instance = new Hierarchy();

                instance.setId((long) datum.get("id"));
                instance.setName((String) datum.get("name"));
                instance.setParentId((Long) datum.get("parent_id"));

                return instance;
            },
            instance -> Map.ofEntries(
                    Map.entry("id", instance.getId()),
                    Map.entry("name", instance.getName()),
                    Map.entry("parent_id", instance.getParentId())
            ),
            instance -> {
                toGroups(instance, hierarchyByParentId, instance.getParentId());
            }
    );

    public Goods(SqlEntities aEntities) {
        super(aEntities);
    }

    public Entity<Long, Hierarchy> getHierarchy() {
        return hierarchy;
    }

}
