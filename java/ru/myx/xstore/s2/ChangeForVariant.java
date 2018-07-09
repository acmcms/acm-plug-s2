/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s2;

/**
 * @author myx
 */
class ChangeForVariant extends ChangeForEntry {
	private final String	variantId;
	
	ChangeForVariant(final CurrentStorage storage, final EntryImpl entry, final String variantId) {
		super( storage, entry );
		this.variantId = variantId;
	}
	
	protected String getDefaultVariant() {
		return this.variantId;
	}
}
