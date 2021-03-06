/**
 * 
 */
package ru.myx.xstore.s2;

/**
 * @author myx
 * 
 */
final class ChangeDoUnlink implements ChangeNested {
	private final EntryImpl	entry;
	
	private final boolean	soft;
	
	/**
	 * @param entry
	 * @param soft
	 */
	ChangeDoUnlink(final EntryImpl entry, final boolean soft) {
		this.entry = entry;
		this.soft = soft;
	}
	
	@Override
	public boolean realCommit(final Transaction transaction) throws Throwable {
		this.entry.getType().onBeforeDelete( this.entry );
		transaction.unlink( this.entry.getOriginalLink(), this.soft );
		// TODO А должно без этого работать!
		final EntryImpl invalidator = (EntryImpl) this.entry.getParent();
		if (invalidator != null) {
			invalidator.invalidateTree();
		}
		return true;
	}
	
}
