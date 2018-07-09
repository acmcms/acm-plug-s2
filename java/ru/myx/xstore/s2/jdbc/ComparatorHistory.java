/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s2.jdbc;

import java.util.Comparator;

final class ComparatorHistory implements Comparator<TreeJdbcEntry> {
	@Override
	public final int compare(final TreeJdbcEntry arg0, final TreeJdbcEntry arg1) {
		final long c0 = arg1.created;
		final long c1 = arg0.created;
		return c0 < c1
				? -1
				: c0 == c1
						? 0
						: 1;
	}
}
