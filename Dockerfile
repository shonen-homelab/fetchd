# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod ./
# No external deps (stdlib only), but keep the layer warm if any appear.
RUN go mod download 2>/dev/null || true
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/fetchd .

# ---- run ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fetchd /fetchd
EXPOSE 8080
ENTRYPOINT ["/fetchd"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["/fetchd", "healthcheck"]
