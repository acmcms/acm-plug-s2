/*
 * Created on 28.01.2005
 */
package ru.myx.xstore.s2.jdbc;

import java.util.Iterator;
import java.util.Map;

import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.help.Create;

final class Differer {
	
	static final Map<String, String> getExtraDiff(final Map<String, String> extrasPassed, final BaseObject data, final String keyPrefix, final Object issuer) {
		
		Map<String, String> extras = extrasPassed;
		for (final Iterator<String> i = Base.keys(data); i.hasNext();) {
			final String key = i.next();
			final BaseObject value = data.baseGet(key, BaseObject.UNDEFINED);
			assert value != null : "NULL java value";
			if (value == BaseObject.UNDEFINED) {
				i.remove();
				continue;
			}
			if (value instanceof External) {
				final External extra = (External) value;
				if (extra.getRecordIssuer() == issuer) {
					if (extras == null) {
						extras = Create.tempMap();
					}
					extras.put(keyPrefix + key, extra.getIdentity());
					continue;
				}
				data.baseDefine(key, Base.forUnknown(extra.baseValue()));
			} else if (value.baseHasKeysOwn()) {
				extras = Differer.getExtraDiff(extras, value, keyPrefix + key + '/', issuer);
			}
		}
		return extras;
	}
	
	static final Map<String, String> getExtraRemoved(final BaseObject removed, final Object issuer) {
		
		Map<String, String> extraRemoved = null;
		for (final Iterator<String> iterator = removed.baseKeysOwn(); iterator.hasNext();) {
			final String key = iterator.next();
			final BaseObject value = removed.baseGet(key, BaseObject.UNDEFINED);
			assert value != null : "NULL java value";
			if (value == BaseObject.UNDEFINED) {
				continue;
			}
			if (value instanceof External) {
				final External extra = (External) value;
				if (extra.getRecordIssuer() == issuer) {
					if (extraRemoved == null) {
						extraRemoved = Create.tempMap();
					}
					extraRemoved.put(key, extra.getIdentity());
					continue;
				}
			}
		}
		return extraRemoved;
	}
}
