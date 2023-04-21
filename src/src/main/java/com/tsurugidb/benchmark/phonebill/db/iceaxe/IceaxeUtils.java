package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
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

	public <T> TsurugiSqlPreparedStatement<T> createPreparedStatement(String sql,
			TgParameterMapping<T> parameterMapping) {
		try {
			return session.createStatement(sql, parameterMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public TsurugiSqlStatement createPreparedStatement(String sql) {
		try {
			return session.createStatement(sql);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}


	public TsurugiSqlQuery<TsurugiResultEntity> createPreparedQuery(String sql) {
		try {
			return session.createQuery(sql);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public <T> TsurugiSqlQuery<T> createPreparedQuery(String sql, TgEntityResultMapping<T> resultMapping) {
		try {
			return session.createQuery(sql, resultMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public <T> TsurugiSqlPreparedQuery<TgBindParameters, T> createPreparedQuery(String sql,
			TgParameterMapping<TgBindParameters> parameterMapping, TgEntityResultMapping<T> resultMapping) {
		try {
			return session.createQuery(sql, parameterMapping, resultMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public TsurugiSqlPreparedQuery<TgBindParameters, TsurugiResultEntity> createPreparedQuery(String sql,
			TgParameterMapping<TgBindParameters> parameterMapping) {
		try {
			return session.createQuery(sql, parameterMapping);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public int executeAndGetCount(TsurugiSqlStatement ps) {
		try (ps){
			return manager.getCurrentTransaction().executeAndGetCount(ps);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> int executeAndGetCount(TsurugiSqlPreparedStatement<T> ps, T t) {
		try (ps){
			return manager.getCurrentTransaction().executeAndGetCount(ps, t);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> int[] executeAndGetCount(TsurugiSqlPreparedStatement<T> ps, Collection<T> c) {
		int[] ret = new int[c.size()];
		try (ps) {
			int i = 0;
			for (T t: c) {
				ret[i++] = manager.getCurrentTransaction().executeAndGetCount(ps, t);
			}
			return ret;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> List<T> execute(TsurugiSqlQuery<T> ps) {
		try (ps) {
			return manager.getCurrentTransaction().executeAndGetList(ps);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}

	public <T> List<T> execute(TsurugiSqlPreparedQuery<TgBindParameters, T> ps, TgBindParameters parameter) {
		try (ps) {
			return manager.getCurrentTransaction().executeAndGetList(ps, parameter);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (TsurugiTransactionException e) {
			throw new TsurugiTransactionRuntimeException(e);
		}
	}
}
