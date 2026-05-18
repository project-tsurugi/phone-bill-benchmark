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

# AWS CLI のインストール
USER root
RUN apt-get update && apt-get install -y \
    unzip curl \
 && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && ./aws/install \
 && rm -rf awscliv2.zip aws

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
