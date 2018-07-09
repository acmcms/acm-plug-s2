/*
 * Created on 14.01.2005
 */
package ru.myx.xstore.s2.jdbc;

import java.util.function.Function;
import ru.myx.ae3.help.Convert;
import ru.myx.query.OneCondition;
import ru.myx.query.OneConditionSimple;

final class ReplacerConditionBooleanToChar implements Function<OneCondition, OneCondition> {
	
	@Override
	public OneCondition apply(final OneCondition condition) {
		
		return new OneConditionSimple(condition.isExact(), condition.getField(), "=", Convert.Any.toInt(condition.getValue(), 0) == 0
			? "N"
			: "Y");
	}
}
