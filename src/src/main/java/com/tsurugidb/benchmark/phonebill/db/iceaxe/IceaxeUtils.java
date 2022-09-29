package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;

/**
 * IceaxeのAPI呼び出し時にチェック例外を非チェック例外でラッピングするutilクラス。
 */
public class IceaxeUtils {
	private final PhoneBillDbManagerIceaxe manager;
	private final TsurugiSession session;

	public IceaxeUtils(PhoneBillDbManagerIceaxe manager) {
		this.manager = manager;
		this.session = manager.getSession();
	}

	public <T> TsurugiPreparedStatementUpdate1<T> createPreparedStatement(String sql,
			TgParameterMapping<T> parameterMapping) {
		try {
			return session.createPreparedStatement(sql, parameterMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public TsurugiPreparedStatementQuery0<TsurugiResultEntity> createPreparedQuery(String sql) {
		try {
			return session.createPreparedQuery(sql);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public <T> TsurugiPreparedStatementQuery0<T> createPreparedQuery(String sql, TgEntityResultMapping<T> resultMapping) {
		try {
			return session.createPreparedQuery(sql, resultMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public <T> TsurugiPreparedStatementQuery1<TgParameterList, T> createPreparedQuery(String sql,
			TgParameterMapping<TgParameterList> parameterMapping, TgEntityResultMapping<T> resultMapping) {
		try {
			return session.createPreparedQuery(sql, parameterMapping, resultMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public TsurugiPreparedStatementQuery1<TgParameterList, TsurugiResultEntity> createPreparedQuery(String sql,
			TgParameterMapping<TgParameterList> parameterMapping) {
		try {
			return session.createPreparedQuery(sql, parameterMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public <T> int executeAndGetCount(TsurugiPreparedStatementUpdate1<T> ps, T t) {
		try (ps){
			return ps.executeAndGetCount(manager.getCurrentTransaction(), t);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> int[] executeAndGetCount(TsurugiPreparedStatementUpdate1<T> ps, Collection<T> c) {
		int[] ret = new int[c.size()];
		try (ps) {
			int i = 0;
			for (T t: c) {
				ret[i++] = ps.executeAndGetCount(manager.getCurrentTransaction(), t);
			}
			return ret;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> List<T> execute(TsurugiPreparedStatementQuery0<T> ps) {
		try (ps; var result = ps.execute(manager.getCurrentTransaction())) {
				return result.getRecordList();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> List<T> execute(TsurugiPreparedStatementQuery1<TgParameterList, T> ps, TgParameterList param) {
		try (ps; var result = ps.execute(manager.getCurrentTransaction(), param)) {
			return result.getRecordList();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}
}
