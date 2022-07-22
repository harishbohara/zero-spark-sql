package io.github.harishb2k.spark.sql.parser.node;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@Data
@RequiredArgsConstructor(onConstructor_ = {@Inject})
public abstract class Node {

    /**
     * Who is my parent
     */
    private Node parent;

    /**
     * All children
     */
    private final List<Node> children = new ArrayList<>();


    /**
     * Add a children to this node
     */
    public void addChildren(Node node) {
        children.add(node);
        node.parent = this;
    }

    /**
     * Describe this node for debugging
     *
     * @param verbose if true then lot of details are added
     */
    public String describe(boolean verbose) {
        if (verbose) {
            return "";
        } else {
            return "";
        }
    }

    @Override
    public String toString() {
        return describe(false);
    }

    public void print(int depth) {
        char t = '\t';

        String s = "";
        if (describe(false) != null) {
            s = describe(false);
        }
        if (depth == 1) {
            System.out.println(s);
        }

        for (Node n : children) {
            s = n.describe(false);
            if (n.describe(false) != null) {
                s = n.describe(false);
            }
            System.out.println(StringUtils.repeat(t, depth) + s);
            n.print(depth + 1);
        }
    }
}
