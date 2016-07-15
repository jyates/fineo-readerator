package io.fineo.drill.exec.store.dynamo.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Tree structured filter specification for building a Dynamo filter
 */
@JsonTypeName("dynamo-filter-spec")
public class DynamoFilterSpec {

  private static final String AND = "AND";
  private static final String OR = "OR";

  // mapping of functions for checking individual columns
  private static final Map<String, Op> COLUMN_FUNCTION_MAP = new HashMap<>();

  static {
    COLUMN_FUNCTION_MAP.put("isNull", new Func("attribute_not_exists"));
    COLUMN_FUNCTION_MAP.put("isNotNull", new Func("attribute_exists"));
    COLUMN_FUNCTION_MAP.put("equal", op("="));
    COLUMN_FUNCTION_MAP.put("not_equal", op("<>"));
    COLUMN_FUNCTION_MAP.put("greater_than_or_equal_to", op(">="));
    COLUMN_FUNCTION_MAP.put("greater_than", op(">"));
    COLUMN_FUNCTION_MAP.put("less_than_or_equal_to", op("<="));
    COLUMN_FUNCTION_MAP.put("less_than", op("<"));
  }

  // functions for interrogating the contents of a document (list, map, set)
  private static final Map<String, Op> DOCUMENT_FUNCTION_MAP = new HashMap<>();

  static {
    DOCUMENT_FUNCTION_MAP.put("isNotNull", new Func2("contains"));
    // count? -> size
  }

  public static DynamoFilterSpec create(String functionName, String fieldName, Object fieldValue) {
    Op op;
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

  public DynamoFilterSpec(){}

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

    public FilterTree(String key, Op operand, Object value) {
      this.root = new FilterLeaf(key, operand, value);
    }

    @JsonIgnore
    public FilterTree and(String key, Op operand, String value) {
      return op(AND, key, operand, value);
    }

    @JsonIgnore
    public FilterTree or(String key, Op operand, String value) {
      return op(OR, key, operand, value);
    }

    private FilterTree op(String op, String key, Op operand, String value) {
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


    public void update(FilterNode node, FilterLeaf leaf) {
      if (node == left) {
        this.left = leaf;
      } else {
        assert this.right == node;
        this.right = leaf;
      }
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitInnerNode(this);
    }
  }

  @JsonTypeName("dynamo-filter-tree-leaf-node")
  public static class FilterLeaf extends FilterNode {
    private String key;
    private Op operand;
    private Object value;

    @JsonCreator
    public FilterLeaf(@JsonProperty("key") String key, @JsonProperty("operand") Op operand,
      @JsonProperty("value") Object value) {
      this.key = key;
      this.operand = operand;
      this.value = value;
    }

    @Override
    public String toString() {
      return operand.compose(key, value.toString());
    }

    @JsonProperty
    public String getKey() {
      return key;
    }

    @JsonProperty
    public Op getOperand() {
      return operand;
    }

    @JsonProperty
    public Object getValue() {
      return value;
    }

    @JsonIgnore
    public boolean registerKey() {
      return operand.registerKey();
    }

    @JsonIgnore
    public boolean registerValue() {
      return operand.registerValue();
    }

    @Override
    public <T> T visit(FilterNodeVisitor<T> visitor) {
      return visitor.visitLeafNode(this);
    }
  }

  private static Op op(String name) {
    return new Op(name);
  }

  @JsonTypeName("dynamo-filter-operand")
  public static class Op {
    private String name;

    @JsonCreator
    public Op(@JsonProperty("name") String name) {
      this.name = name;
    }

    @JsonProperty
    public String getName() {
      return name;
    }

    @JsonIgnore
    public String compose(String key, String value) {
      return format("%s %s %s", key, name, value);
    }

    @JsonIgnore
    public boolean registerKey() {
      return true;
    }

    @JsonIgnore
    public boolean registerValue() {
      return true;
    }
  }

  @JsonTypeName("dynamo-filter-operand_function")
  public static class Func extends Op {

    public Func(@JsonProperty("name") String name) {
      super(name);
    }

    @Override
    public String compose(String key, String value) {
      return format("%s(%s)", getName(), key);
    }

    @Override
    public boolean registerValue() {
      return false;
    }
  }

  @JsonTypeName("dynamo-filter-operand_function2")
  public static class Func2 extends Op {

    public Func2(@JsonProperty("name") String name) {
      super(name);
    }

    @Override
    public String compose(String key, String value) {
      return format("%s(%s, %s)", getName(), key, value);
    }
  }

  public interface FilterNodeVisitor<T> {
    T visitInnerNode(FilterNodeInner inner);

    T visitLeafNode(FilterLeaf leaf);
  }
}
