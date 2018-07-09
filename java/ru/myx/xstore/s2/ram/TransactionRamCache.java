/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import java.util.Set;

import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.help.Create;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Transaction;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class TransactionRamCache implements Transaction {
	private final ServerRamCache	cache;
	
	private final Transaction		transaction;
	
	private Set<String>				clearLinks	= null;
	
	TransactionRamCache(final ServerRamCache cache, final Transaction transaction) {
		this.cache = cache;
		this.transaction = transaction;
	}
	
	@Override
	public void aliases(final String guid, final Set<String> aliasAdd, final Set<String> aliasRemove) throws Throwable {
		if (aliasAdd != null) {
			for (final String alias : aliasAdd) {
				this.clearLink( alias );
			}
		}
		if (aliasRemove != null) {
			for (final String alias : aliasRemove) {
				this.clearLink( alias );
			}
		}
		this.clearLink( guid );
		this.transaction.aliases( guid, aliasAdd, aliasRemove );
	}
	
	final void clearLink(final String lnkId) {
		if (this.clearLinks == null) {
			this.clearLinks = Create.tempSet();
		}
		this.clearLinks.add( lnkId );
	}
	
	@Override
	public void commit() {
		try {
			this.transaction.commit();
		} finally {
			this.cache.clear( this.clearLinks );
		}
	}
	
	@Override
	public void create(
			final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String name,
			final boolean folder,
			final long created,
			final String owner,
			final int state,
			final String title,
			final String typeName,
			final BaseObject data,
			final String versionId,
			final String versionComment,
			final BaseObject versionData) throws Throwable {
		this.transaction.create( local,
				ctnLnkId,
				lnkId,
				name,
				folder,
				created,
				owner,
				state,
				title,
				typeName,
				data,
				versionId,
				versionComment,
				versionData );
	}
	
	@Override
	public void delete(final BaseLink link, final boolean soft) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.delete( ((LinkCached) link).getParentLink(), soft );
		} else {
			this.transaction.delete( link, soft );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
	}
	
	@Override
	public void link(
			final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String name,
			final boolean folder,
			final String linkedIdentity) throws Throwable {
		this.transaction.link( local, ctnLnkId, lnkId, name, folder, linkedIdentity );
		this.clearLink( linkedIdentity );
	}
	
	@Override
	public void move(final BaseLink link, final String ctnLnkId, final String key) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.move( ((LinkCached) link).getParentLink(), ctnLnkId, key );
		} else {
			this.transaction.move( link, ctnLnkId, key );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
		this.clearLink( ctnLnkId );
	}
	
	@Override
	public void record(final String linkedIdentity) throws Throwable {
		this.transaction.record( linkedIdentity );
	}
	
	@Override
	public void rename(final BaseLink link, final String key) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.rename( ((LinkCached) link).getParentLink(), key );
		} else {
			this.transaction.rename( link, key );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
	}
	
	@Override
	public void resync(final String lnkId) throws Throwable {
		this.transaction.resync( lnkId );
	}
	
	@Override
	public void revert(
			final BaseLink link,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.revert( ((LinkCached) link).getParentLink(),
					historyId,
					folder,
					created,
					state,
					title,
					typeName );
		} else {
			this.transaction.revert( link, historyId, folder, created, state, title, typeName );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
	}
	
	@Override
	public void revert(
			final BaseLink link,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final BaseObject removed,
			final BaseObject added) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.revert( ((LinkCached) link).getParentLink(),
					historyId,
					folder,
					created,
					state,
					title,
					typeName,
					removed,
					added );
		} else {
			this.transaction.revert( link, historyId, folder, created, state, title, typeName, removed, added );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
	}
	
	@Override
	public void rollback() {
		try {
			this.transaction.rollback();
		} finally {
			this.cache.clear( this.clearLinks );
		}
	}
	
	@Override
	public void segregate(final String guid, final String linkedIdentityOld, final String linkedIdentityNew)
			throws Throwable {
		this.clearLink( guid );
		this.transaction.segregate( guid, linkedIdentityOld, linkedIdentityNew );
	}
	
	@Override
	public void unlink(final BaseLink link, final boolean soft) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.unlink( ((LinkCached) link).getParentLink(), soft );
		} else {
			this.transaction.unlink( link, soft );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( link.getLinkedIdentity() );
	}
	
	@Override
	public void update(final BaseLink link, final String linkedIdentity) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.update( ((LinkCached) link).getParentLink(), linkedIdentity );
		} else {
			this.transaction.update( link, linkedIdentity );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( linkedIdentity );
	}
	
	@Override
	public void update(
			final BaseLink link,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.update( ((LinkCached) link).getParentLink(),
					linkedIdentity,
					versionId,
					folder,
					created,
					state,
					title,
					typeName,
					ownership );
		} else {
			this.transaction.update( link,
					linkedIdentity,
					versionId,
					folder,
					created,
					state,
					title,
					typeName,
					ownership );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( linkedIdentity );
	}
	
	@Override
	public void update(
			final BaseLink link,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership,
			final BaseObject removed,
			final BaseObject added) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.update( ((LinkCached) link).getParentLink(),
					linkedIdentity,
					versionId,
					folder,
					created,
					state,
					title,
					typeName,
					ownership,
					removed,
					added );
		} else {
			this.transaction.update( link,
					linkedIdentity,
					versionId,
					folder,
					created,
					state,
					title,
					typeName,
					ownership,
					removed,
					added );
		}
		this.clearLink( link.getGuid() );
		this.clearLink( linkedIdentity );
	}
	
	@Override
	public void versionClearAll(final BaseLink link) throws Throwable {
		if (link instanceof LinkCached) {
			this.transaction.versionClearAll( ((LinkCached) link).getParentLink() );
		} else {
			this.transaction.versionClearAll( link );
		}
	}
	
	@Override
	public void versionCreate(
			final String versionId,
			final String versionParentId,
			final String versionComment,
			final String objectId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject versionData) throws Throwable {
		this.transaction.versionCreate( versionId,
				versionParentId,
				versionComment,
				objectId,
				title,
				typeName,
				owner,
				versionData );
	}
	
	@Override
	public void versionStart(
			final String versionId,
			final String versionComment,
			final String objectId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject versionData) throws Throwable {
		this.transaction.versionStart( versionId, versionComment, objectId, title, typeName, owner, versionData );
	}
}
