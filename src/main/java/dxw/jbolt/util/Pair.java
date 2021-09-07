package dxw.jbolt.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pair<L, R> implements Serializable {
    private static final long serialVersionUID = 6077505257778152411L;

    public L left;
    public R right;

    public static final <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    public static final <L, R> Pair<L, R> of(Map.Entry<L, R> entry) {
        return new Pair<>(entry.getKey(), entry.getValue());
    }

}
