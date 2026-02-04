# ==========================================
# Stage 1: Build Stage
# ==========================================
FROM eclipse-temurin:17-jdk-jammy AS builder

WORKDIR /app

# プロジェクト構造に合わせて src 以下の Gradle 設定ファイルをコピー
# フォルダが存在しないエラーを避けるため、ワイルドカードやsrc指定を使います
COPY src/gradlew src/build.gradle src/settings.gradle /app/
COPY src/gradle /app/gradle

RUN chmod +x ./gradlew

# 依存関係のキャッシュ
RUN ./gradlew --no-daemon dependencies || true

# 残りのソースコードをコピーしてビルド
COPY src/src /app/src
RUN ./gradlew distTar --no-daemon --info --stacktrace

# ==========================================
# Stage 2: Runtime Stage
# ==========================================
FROM ghcr.io/project-tsurugi/tsurugidb:1.7.0-ubuntu-24.04

WORKDIR /app

# ビルダーからビルド済み成果物（tar.gz）のみをコピー
COPY --from=builder /app/build/distributions/phone-bill.tar.gz /app/

# 展開して元ファイルを削除
RUN tar xf phone-bill.tar.gz && rm phone-bill.tar.gz

# エントリポイントスクリプトの作成
# sleep 5 よりも確実な「起動待ち」を検討（後述）
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
# TsurugiDBをバックグラウンドで起動\n\
/usr/local/bin/docker-entrypoint.sh &\n\
\n\
# TsurugiDBの準備ができるまで待機（ヘルスチェック等の代用）\n\
echo "Waiting for TsurugiDB to initialize..."\n\
for i in {1..30}; do\n\
    if tgctl status > /dev/null 2>&1; then\n\
        echo "TsurugiDB is ready!"\n\
        break\n\
    fi\n\
    sleep 2\n\
done\n\
\n\
# 全ての引数をコマンドとして実行\n\
exec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/bin/bash"]
