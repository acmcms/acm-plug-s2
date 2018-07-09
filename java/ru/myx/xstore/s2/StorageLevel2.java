/*
 * Created on 27.06.2004
 */
package ru.myx.xstore.s2;

import java.io.File;
import java.sql.Connection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;

import ru.myx.ae1.AbstractPluginInstance;
import ru.myx.ae1.control.Control;
import ru.myx.ae1.control.MultivariantString;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.storage.ModuleRecycler;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae1.storage.PluginRegistry;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.Engine;
import java.util.function.Function;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.base.BaseProperty;
import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CacheL3;
import ru.myx.ae3.control.command.ControlCommand;
import ru.myx.ae3.control.command.ControlCommandset;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.xstore.basics.ControlNodeImpl;
import ru.myx.xstore.basics.StorageSynchronizationActorProvider;
import ru.myx.xstore.s2.jdbc.ServerJdbc;
import ru.myx.xstore.s2.local.ServerLocal;
import ru.myx.xstore.s2.ram.ServerRamCache;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public final class StorageLevel2 extends AbstractPluginInstance implements StorageImpl {
	
	
	private static final ControlCommand<?> CMD_CLEAR_CACHES = Control
			.createCommand("clear_cache", MultivariantString.getString("Clear storage caches", Collections.singletonMap("ru", "Очистить кеши хранилища")))
			.setCommandPermission("publish").setCommandIcon("command-dispose");
	
	private static final ControlCommand<?> CMD_CLEAR_TEMPLATE_CACHES = Control
			.createCommand("clear_template_cache", MultivariantString.getString("Clear template caches", Collections.singletonMap("ru", "Очистить кеши шаблонов")))
			.setCommandPermission("publish").setCommandIcon("command-dispose");
	
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	//
	// Plugin methods
	//
	//
	private CacheL2<Object> cacheRendered;

	private CurrentStorage currentStorage;

	private Server server;

	private String storageId;

	private String interfaceId;

	private String prefix;

	private String connection;

	private File cacheRoot;

	private String storageIdentity;

	private Enumeration<Connection> connectionSource;

	private boolean scheduling;

	private boolean update3;

	private String controlLocation;

	@Override
	public final boolean areAliasesSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areLinksSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areLocksSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areObjectHistoriesSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areObjectVersionsSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areSchedulesSupported() {
		
		
		return this.currentStorage != null && this.currentStorage.areSchedulesSupported();
	}

	@Override
	public final boolean areSoftDeletionsSupported() {
		
		
		return true;
	}

	@Override
	public final boolean areSynchronizationsSupported() {
		
		
		return this.currentStorage.areSynchronizationsSupported();
	}

	/**
	 * @return message
	 */
	public final Object clearCacheAll() {
		
		
		this.server.invalidateCache();
		this.cacheRendered.clear();
		this.getServer().getObjectCache().clear();
		return MultivariantString.getString("All storage caches were discarded.", Collections.singletonMap("ru", "Кеши хранилища успешно очищены."));
	}

	/**
	 * @return message
	 */
	public final Object clearCacheTypes() {
		
		
		this.getServer().getObjectCache().clear();
		return MultivariantString.getString("All template caches were discarded.", Collections.singletonMap("ru", "Кеши шаблонов успешно очищены."));
	}

	@Override
	public final void destroy() {
		
		
		PluginRegistry.removePlugin(this);
		this.currentStorage.stop();
	}

	@Override
	public final Object getCommandResult(final ControlCommand<?> command, final BaseObject arguments) {
		
		
		if (command == StorageLevel2.CMD_CLEAR_CACHES) {
			{
				final BaseObject result = new BaseNativeObject()//
						.putAppend("storage", this.getMnemonicName());
				this.getServer().logQuickTaskUsage("XDS_COMMAND_STORAGE(2)_CC", result);
			}
			return this.clearCacheAll();
		}
		if (command == StorageLevel2.CMD_CLEAR_TEMPLATE_CACHES) {
			{
				final BaseObject result = new BaseNativeObject()//
						.putAppend("storage", this.getMnemonicName());
				this.getServer().logQuickTaskUsage("XDS_COMMAND_STORAGE(2)_CT", result);
			}
			return this.clearCacheTypes();
		}
		throw new IllegalArgumentException("Unknown command: " + command.getKey());
	}

	@Override
	public ControlCommandset getCommands() {
		
		
		final ControlCommandset result = Control.createOptions();
		result.add(StorageLevel2.CMD_CLEAR_CACHES);
		result.add(StorageLevel2.CMD_CLEAR_TEMPLATE_CACHES);
		return result;
	}

	@Override
	public final Enumeration<Connection> getConnectionSource() {
		
		
		return this.connectionSource;
	}

	@Override
	public final ControlCommandset getContentCommands(final String key) {
		
		
		return null;
	}

	/**
	 * @return identity
	 */
	public String getIdentity() {
		
		
		return this.storageIdentity;
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	// /////////////////////////////////////////////////////////////////////////////////////////////////////////
	//
	//
	// Impl methods
	//
	//
	/**
	 * @return result
	 */
	public final IndexingDictionary getIndexingDictionary() {
		
		
		return this.server.getIndexingDictionary();
	}

	/**
	 * @return result
	 */
	public final IndexingStemmer getIndexingStemmer() {
		
		
		return this.server.getIndexingStemmer();
	}

	@Override
	public final ModuleInterface getInterface() {
		
		
		return this.currentStorage;
	}

	@Override
	public String getLocationControl() {
		
		
		return this.controlLocation;
	}

	@Override
	public final LockManager getLocker() {
		
		
		return this.currentStorage;
	}

	@Override
	public final String getMnemonicName() {
		
		
		return this.storageId;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <T> CacheL2<T> getObjectCache() {
		
		
		return (CacheL2<T>) this.cacheRendered;
	}

	@Override
	public final ModuleRecycler getRecycler() {
		
		
		return this.currentStorage;
	}

	@Override
	public final ModuleSchedule getScheduling() {
		
		
		return this.currentStorage.getScheduling();
	}

	/**
	 * @return server
	 */
	public final Server getServerImpl() {
		
		
		return this.server;
	}

	@Override
	public final ModuleInterface getStorage() {
		
		
		return this.currentStorage;
	}

	@Override
	public final ModuleSynchronizer getSynchronizer() {
		
		
		return this.currentStorage.getSynchronizer();
	}

	@Override
	public final Connection nextConnection() {
		
		
		return this.connectionSource.nextElement();
	}

	@Override
	public final void register() {
		
		
		if (this.update3) {
			Report.info("STORAGE2", "Storage in update3 mode!");
		}
		this.connectionSource = this.getServer().getConnections().get(this.connection);
		final CacheL3<Object> cache = this.getServer().getCache();
		final String cachePrefix = this.getServer().getZoneId() + '-' + this.storageId;
		this.cacheRendered = this.getServer().getObjectCache();
		{
			String storageIdentity = "";
			final BaseObject settingsPrivate = this.getSettingsPrivate();
			storageIdentity = Base.getString(settingsPrivate, "identity", "").trim();
			if (storageIdentity.length() == 0) {
				storageIdentity = Engine.createGuid();
				settingsPrivate.baseDefine("identity", storageIdentity);
				this.commitPrivateSettings();
			}
			this.storageIdentity = storageIdentity;
		}
		try {
			this.cacheRoot = new File(new File(Engine.PATH_CACHE, this.getServer().getZoneId()), this.storageId);
			this.cacheRoot.mkdirs();
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
		this.server = new ServerRamCache(
				cache,
				cachePrefix,
				new ServerLocal(this, this.cacheRoot, new ServerJdbc(this, this.prefix, this.storageIdentity, this.scheduling, this.update3)));
		this.currentStorage = new CurrentStorage(this, this.server);
		this.controlLocation = '/' + this.getMnemonicName() + '/';
		this.getServer().getControlRoot().bind(new ControlNodeImpl(this, this.getStorage().getRootIdentifier(), true));
		this.getServer().registerCommonActor(new StorageSynchronizationActorProvider(this.getServer(), this.controlLocation));
		final Map<String, Function<Void, Object>> registrySignals = this.getServer().registrySignals();
		registrySignals.put("S2_CLEAR_CACHE." + this.getMnemonicName(), new SignalCommand(this, StorageLevel2.CMD_CLEAR_CACHES));
		registrySignals.put("S2_REFRESH_TYPES." + this.getMnemonicName(), new SignalCommand(this, StorageLevel2.CMD_CLEAR_TEMPLATE_CACHES));
	}

	@Override
	public final void setup() {
		
		
		final BaseObject info = this.getSettingsProtected();
		this.storageId = Base.getString(info, "id", "s2");
		this.interfaceId = Base.getString(info, "api", "s2");
		this.prefix = Base.getString(info, "prefix", "s2");
		this.connection = Base.getString(info, "connection", "default");
		this.scheduling = Convert.MapEntry.toBoolean(info, "scheduling", true);
		this.update3 = Convert.MapEntry.toBoolean(info, "update3", false);
	}

	@Override
	public final void start() {
		
		
		final BaseObject reflection = this.getServer().getRootContext().ri10GV;
		final BaseObject baseStorage = Base.forUnknown(this.currentStorage);
		reflection.baseDefine(this.interfaceId, baseStorage, BaseProperty.ATTRS_MASK_NNN);
		reflection.baseDefine(this.storageId, baseStorage, BaseProperty.ATTRS_MASK_NNN);
		if (reflection.baseGetOwnProperty("Storage") == null) {
			reflection.baseDefine("Storage", baseStorage, BaseProperty.ATTRS_MASK_NNN);
		}
		this.currentStorage.start();
		PluginRegistry.addPlugin(this);
	}

	@Override
	public final String toString() {
		
		
		return "S2{" + this.getMnemonicName() + '}';
	}
}
