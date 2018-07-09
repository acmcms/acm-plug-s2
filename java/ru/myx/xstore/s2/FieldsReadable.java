/*
 * Created on 05.07.2004
 */
package ru.myx.xstore.s2;

import java.util.Iterator;

import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseHostSealed;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BasePrimitive;
import ru.myx.ae3.base.BasePrimitiveString;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.exec.ExecProcess;
import ru.myx.ae3.exec.ExecStateCode;
import ru.myx.ae3.exec.ResultHandler;

/**
 * @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
class FieldsReadable extends BaseHostSealed implements BaseProperty {
	
	private final EntryImpl entry;

	private BaseObject parent = null;

	FieldsReadable(final EntryImpl entry) {
		this.entry = entry;
	}

	@Override
	public BaseProperty baseGetOwnProperty(final BasePrimitiveString key) {
		
		if (key.length() > 0 && '$' == key.charAt(0)) {
			return this;
		}
		return this.getParent().baseFindProperty(key, BaseObject.PROTOTYPE);
	}

	@Override
	public BaseProperty baseGetOwnProperty(final String key) {
		
		if (key.length() > 0 && '$' == key.charAt(0)) {
			return this;
		}
		return this.getParent().baseFindProperty(key, BaseObject.PROTOTYPE);
	}

	@Override
	public boolean baseHasKeysOwn() {
		
		return this.getParent().baseHasKeysOwn();
	}

	@Override
	public Iterator<String> baseKeysOwn() {
		
		return this.getParent().baseKeysOwn();
	}

	@Override
	public Iterator<? extends BasePrimitive<?>> baseKeysOwnPrimitive() {
		
		return this.getParent().baseKeysOwnPrimitive();
	}

	@Override
	public BaseObject basePrototype() {
		
		return null;
	}

	private BaseObject getParent() {
		
		return this.parent == null
			? this.parent = this.entry.getDataReal()
			: this.parent;
	}

	@Override
	public short propertyAttributes(final CharSequence name) {
		
		return BaseProperty.ATTRS_MASK_NNN_NNK;
	}

	@Override
	public BaseObject propertyGet(final BaseObject instance, final BasePrimitiveString key) {
		
		return this.propertyGet(null, key.toString());
	}

	@Override
	public BaseObject propertyGet(final BaseObject instance, final String key) {
		
		if ("$key".equals(key)) {
			return Base.forString(this.entry.getKey());
		}
		if ("$title".equals(key)) {
			return Base.forString(this.entry.getTitle());
		}
		if ("$state".equals(key)) {
			return Base.forInteger(this.entry.getState());
		}
		if ("$folder".equals(key)) {
			return this.entry.isFolder()
				? BaseObject.TRUE
				: BaseObject.FALSE;
		}
		if ("$type".equals(key)) {
			return Base.forString(this.entry.getTypeName());
		}
		if ("$created".equals(key)) {
			return Base.forDateMillis(this.entry.getCreated());
		}
		if ("$modified".equals(key)) {
			return Base.forDateMillis(this.entry.getModified());
		}
		if ("$owner".equals(key)) {
			return Base.forString(this.entry.getOwner());
		}
		return BaseObject.UNDEFINED;
	}

	@Override
	public BaseObject propertyGetAndSet(final BaseObject instance, final String name, final BaseObject value) {
		
		return this.propertyGet(null, name);
	}

	@Override
	public ExecStateCode propertyGetCtxResult(final ExecProcess ctx, final BaseObject instance, final BasePrimitive<?> name, final ResultHandler store) {
		
		return store.execReturn(ctx, this.propertyGet(null, name.toString()));
	}

}
