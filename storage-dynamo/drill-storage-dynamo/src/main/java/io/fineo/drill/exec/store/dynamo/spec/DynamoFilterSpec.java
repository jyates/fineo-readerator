package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Tree structured filter specification for building a Dynamo filter
 */
@JsonTypeName("dynamo-filter-spec")
public class DynamoFilterSpec {

  private static final String AND = "AND";
  private static final String OR = "OR";

  // mapping of functions for checking individual columns
  private static final Map<String, Function<LeafInfo, FilterLeaf>> COLUMN_FUNCTION_MAP = new
    HashMap<>();

  static {
    COLUMN_FUNCTION_MAP.put("isNull", func0("attribute_not_exists"));
    COLUMN_FUNCTION_MAP.put("isNotNull", func0("attribute_exists"));
    COLUMN_FUNCTION_MAP.put("between",
      info -> new FilterLeaf(info.key, "BETWEEN", "%1$s %2$s %3$s AND %4$s", info.values));
    COLUMN_FUNCTION_MAP.put("equal", op("="));
    COLUMN_FUNCTION_MAP.put("not_equal", op("<>"));
    COLUMN_FUNCTION_MAP.put("greater_than_or_equal_to", op(">="));
    COLUMN_FUNCTION_MAP.put("greater_than", op(">"));
    COLUMN_FUNCTION_MAP.put("less_than_or_equal_to", op("<="));
    COLUMN_FUNCTION_MAP.put("less_than", op("<"));
  }

  // functions for interrogating the contents of a document (list, map, set)
  private static final Map<String, Function<LeafInfo, FilterLeaf>> DOCUMENT_FUNCTION_MAP =
    new HashMap<>();

  static {
    DOCUMENT_FUNCTION_MAP.put("isNotNull", func0("contains"));
    // count? -> size
  }

  private static class LeafInfo {
    private String key;
    private Object[] values;

    public LeafInfo(String fieldName, Object[] fieldValue) {
      this.key = fieldName;
      this.values = fieldValue;
    }
  }

  private static Function<LeafInfo, FilterLeaf> op(String name) {
    return info -> new FilterLeaf(info.key, name, "%s %s %s", info.values);
  }

  private static Function<LeafInfo, FilterLeaf> func0(String name) {
    return info -> new FilterLeaf(info.key, name, "%2$s(%1$s)");
  }


  public static DynamoFilterSpec copy(FilterLeaf leaf) {
    return new DynamoFilterSpec(new FilterTree(leaf.deepClone()));
  }

  public static DynamoFilterSpec create(String functionName, String fieldName, Object...
    fieldValue) {
    Function<LeafInfo, FilterLeaf> func;
    LeafInfo info = new LeafInfo(fieldName, fieldValue);
    // map or list
    if (fieldName.contains(".") || fieldName.contains("[")) {
      func = DOCUMENT_FUNCTION_MAP.get(functionName);
    } else {
      func = COLUMN_FUNCTION_MAP.get(functionName);
    }
    return func == null ? null : new DynamoFilterSpec(new FilterTree(func.apply(info)));
  }

  private FilterTree tree;

  @JsonCreator
  public DynamoFilterSpec(FilterTree tree) {
    this.tree = tree;
  }

  public DynamoFilterSpec() {
  }

  @JsonIgnore
  public DynamoFilterSpec and(DynamoFilterSpec rightKey) {
    if (rightKey == null) {
      return this;
    }
    this.tree.and(rightKey.tree.getRoot());
    return this;
  }

  @JsonIgnore
  public DynamoFilterSpec or(DynamoFilterSpec rightKey) {
    if (rightKey == null) {
      return this;
    }
    this.tree.or(rightKey.tree.getRoot());
    return this;
  }

  public FilterTree getTree() {
    return tree;
  }

  @JsonIgnore
  public DynamoFilterSpec deepClone() {
    FilterNode root = this.getTree().visit(new FilterNodeVisitor<FilterNode>() {
      @Override
      public FilterNode visitInnerNode(FilterNodeInner inner) {
        FilterNode left = inner.getLeft().visit(this);
        FilterNode right = inner.getRight().visit(this);
        FilterNode innerCopy = new FilterNodeInner(inner.getCondition(), left, right);
        return innerCopy;
      }

      @Override
      public FilterNode visitLeafNode(FilterLeaf leaf) {
        return leaf.deepClone();
      }
    });
    FilterTree copy = new FilterTree(root);
    return new DynamoFilterSpec(copy);
  }

  @Override
  public String toString() {
    return "DynamoFilterSpec{" + tree + '}';
  }

  @JsonTypeName("dyamo-filter-tree")
  public static class FilterTree {
    private FilterNode root;

    @JsonCreator
    public FilterTree(@JsonProperty("root") FilterNode root) {
      this.root = root;
    }

    private FilterTree op(String op, FilterNode right) {
      FilterNodeInner inner = new FilterNodeInner(op, root, right);
      root = inner;
      return this;
    }

    public FilterTree and(FilterNode root) {
      return op(AND, root);
    }

    public FilterTree or(FilterNode root) {
      return op(OR, root);
    }

    public String toString() {
      return this.root.toString();
    }

    @JsonProperty
    public FilterNode getRoot() {
      return root;
    }

    public <T> T visit(FilterNodeVisitor<T> visitor) {
      if (this.root == null) {
        return null;
      }
      FilterNode node = getRoot();
      return node.visit(visitor);
    }
  }

  public abstract static class FilterNode {
    private FilterNode parent;

    @JsonIgnore
    public void setParent(FilterNode parent) {
      this.parent = parent;
    }

    @JsonIgnore
    public FilterNode getParent() {
      return parent;
    }

    public abstract <T> T visit(FilterNodeVisitor<T> visitor);
  }

  @JsonTypeName("dynamo-filter-tree-inner-node")
  public static class FilterNodeInner extends FilterNode {
    private String condition;
    private FilterNode left;
    private FilterNode right;

    public FilterNodeInner(@JsonProperty("condition") String bool,
      @JsonProperty("left") FilterNode left, @JsonProperty("right") FilterNode right) {
      this.condition = bool;
      this.left = left;
      left.setParent(this);
      this.right = right;
      right.setParent(this);
    }

    @Override
    public String toString() {
      return "( " + left.toString() + " ) " + condition + " ( " + right.toString() + " )";
    }

    @JsonProperty
    public String getCondition() {
      return condition;
    }

    @JsonProperty
    public FilterNode getLeft() {
      return left;
    }

    @JsonProperty
    public FilterNode getRight() {
      return right;
    }

    @JsonIgnore
    public boolean and() {
      return this.condition.equals(AND);
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitInnerNode(this);
    }
  }

  @JsonTypeName("dynamo-filter-tree-leaf")
  public static class FilterLeaf extends FilterNode {
    protected String operand;
    protected String format;
    protected String key;
    protected Object[] values;

    public FilterLeaf(String key, String operand, String format, Object... values) {
      this.format = format;
      this.operand = operand;
      this.key = key;
      this.values = values;
    }

    @JsonProperty
    public String getKey() {
      return key;
    }

    @JsonProperty
    public String getOperand() {
      return this.operand;
    }

    @JsonProperty
    public String getFormat() {
      return this.format;
    }

    @JsonProperty
    public Object[] getValues() {
      return values;
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitLeafNode(this);
    }

    @JsonIgnore
    public FilterNode deepClone() {
      return new FilterLeaf(key, operand, format, values);
    }

    public void setKey(String key) {
      this.key = key;
    }


    @Override
    public String toString() {
      Object[] formatted = new Object[values == null ? 2 : values.length + 2];
      formatted[0] = key;
      formatted[1] = operand;
      System.arraycopy(values, 0, formatted, 2, values.length);
      return String.format(format, formatted);
    }

    public void setValue(int i, String value) {
      this.values[i] = value;
    }
  }

  public interface FilterNodeVisitor<T> {
    T visitInnerNode(FilterNodeInner inner);

    T visitLeafNode(FilterLeaf leaf);
  }
}
