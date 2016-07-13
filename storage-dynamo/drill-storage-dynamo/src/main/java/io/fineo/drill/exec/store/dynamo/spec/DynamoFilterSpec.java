package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;
import java.util.Map;

/**
 * Tree structured filter specification for building a Dynamo filter
 */
@JsonTypeName("dynamo-filter-spec")
public class DynamoFilterSpec {

  private static final String AND = "AND";
  private static final String OR = "OR";

  // mapping of functions for checking individual columns
  private static final Map<String, String> COLUMN_FUNCTION_MAP = new HashMap<>();

  static {
    COLUMN_FUNCTION_MAP.put("isNull", "attribute_not_exists");
    COLUMN_FUNCTION_MAP.put("isNotNull", "attribute_exists");
    COLUMN_FUNCTION_MAP.put("equal", "=");
    COLUMN_FUNCTION_MAP.put("not_equal", "<>");
    COLUMN_FUNCTION_MAP.put("greater_than_or_equal_to", ">=");
    COLUMN_FUNCTION_MAP.put("greater_than", ">");
    COLUMN_FUNCTION_MAP.put("less_than_or_equal_to", "<=");
    COLUMN_FUNCTION_MAP.put("less_than", "<");
  }

  // functions for interrogating the contents of a document (list, map, set)
  private static final Map<String, String> DOCUMENT_FUNCTION_MAP = new HashMap<>();

  static {
    DOCUMENT_FUNCTION_MAP.put("isNotNull", "contains");
    // count? -> size
  }

  public static DynamoFilterSpec create(String functionName, String fieldName, Object fieldValue) {
    String op;
    // map or list
    if (fieldName.contains(".") || fieldName.contains("[")) {
      op = DOCUMENT_FUNCTION_MAP.get(functionName);
    } else {
      op = COLUMN_FUNCTION_MAP.get(functionName);
    }
    return op == null ? null : new DynamoFilterSpec(new FilterTree(fieldName, op, fieldValue));
  }

  private FilterTree tree;

  @JsonCreator
  public DynamoFilterSpec(FilterTree tree) {
    this.tree = tree;
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

  @JsonTypeName("dyamo-filter-tree")
  public static class FilterTree {
    private FilterNode root;

    @JsonCreator
    public FilterTree(@JsonProperty("root") FilterNode root) {
      this.root = root;
    }

    public FilterTree(String key, String operand, Object value) {
      this.root = new FilterLeaf(key, operand, value);
    }

    @JsonIgnore
    public FilterTree and(String key, String operand, String value) {
      return op(AND, key, operand, value);
    }

    @JsonIgnore
    public FilterTree or(String key, String operand, String value) {
      return op(OR, key, operand, value);
    }

    private FilterTree op(String op, String key, String operand, String value) {
      FilterLeaf right = new FilterLeaf(key, operand, value);
      return op(op, right);
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
  }

  public static class FilterNode {
    private FilterNode parent;

    @JsonIgnore
    public void setParent(FilterNode parent) {
      this.parent = parent;
    }

    @JsonIgnore
    public FilterNode getParent() {
      return parent;
    }
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

    public void left(FilterLeaf leaf) {
      this.left = leaf;
    }

    public void right(FilterLeaf leaf) {
      this.right = leaf;
    }

    public void update(FilterNode node, FilterLeaf leaf) {
      if (node == left) {
        this.left = leaf;
      } else {
        assert this.right == node;
        this.right = leaf;
      }
    }
  }

  @JsonTypeName("dynamo-filter-tree-left-node")
  public static class FilterLeaf extends FilterNode {
    private String key;
    private String operand;
    private Object value;

    @JsonCreator
    public FilterLeaf(@JsonProperty("key") String key, @JsonProperty("operand") String operand,
      @JsonProperty("value") Object value) {
      this.key = key;
      this.operand = operand;
      this.value = value;
    }

    @Override
    public String toString() {
      return key.toString() + " " + operand + " " + value;
    }

    @JsonProperty
    public String getKey() {
      return key;
    }

    @JsonProperty
    public String getOperand() {
      return operand;
    }

    @JsonProperty
    public Object getValue() {
      return value;
    }
  }

}
