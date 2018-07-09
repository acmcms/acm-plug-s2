/*
 * Created on 07.09.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.local;

import java.io.File;

import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class ExternalizerLocal implements ExternalHandler {
	private static final int			RADIX		= 36;
	
	private static final int			SIZE_LEAF	= 1 << 8;
	
	private static final int			MASK_LEAF	= ExternalizerLocal.SIZE_LEAF - 1;
	
	private final ExternalizerLeaf[]	leafs;
	
	private final ExternalHandler		parent;
	
	ExternalizerLocal(final ServerLocal server, final ExternalHandler parent, final Object issuer, final File folder) {
		this.parent = parent;
		this.leafs = new ExternalizerLeaf[ExternalizerLocal.SIZE_LEAF];
		for (int i = ExternalizerLocal.MASK_LEAF; i >= 0; --i) {
			this.leafs[i] = new ExternalizerLeaf( server, parent, issuer, new File( folder,
					(i < ExternalizerLocal.RADIX
							? "0" + Integer.toString( i, ExternalizerLocal.RADIX )
							: Integer.toString( i, ExternalizerLocal.RADIX )) ) );
		}
	}
	
	final void check(final int index) throws Throwable {
		final ExternalizerLeaf leaf = this.leafs[index & ExternalizerLocal.MASK_LEAF];
		leaf.check();
	}
	
	@Override
	public boolean checkIssuer(final Object issuer) {
		return issuer != null && issuer == this.leafs[0].issuer;
	}
	
	@Override
	public final External getExternal(final Object attachment, final String identifier) throws Exception {
		final ExternalizerLeaf leaf = this.leafs[(identifier.hashCode() & ExternalizerLocal.MASK_LEAF)];
		if (attachment == null) {
			return new ExtraLocal( identifier, leaf );
		}
		return leaf.getExternal( attachment, identifier );
	}
	
	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {
		final ExternalizerLeaf leaf = this.leafs[(identifier.hashCode() & ExternalizerLocal.MASK_LEAF)];
		return leaf.hasExternal( identifier ) || this.parent.hasExternal( attachment, identifier );
	}
	
	@Override
	public final String putExternal(
			final Object attachment,
			final String key,
			final String type,
			final TransferCopier copier) throws Exception {
		final String identifier = this.parent.putExternal( attachment, key, type, copier );
		final ExternalizerLeaf leaf = this.leafs[(identifier.hashCode() & ExternalizerLocal.MASK_LEAF)];
		leaf.putExternal( identifier, type, copier );
		return identifier;
	}
}
