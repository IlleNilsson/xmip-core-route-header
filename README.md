# xmip-core-route-header

Header route technology: `header:<protocol>.<name>` reads a header the transport delivered beside the bytes, from the `<protocol>.header.<name>` context key `context::property::header` builds; the name's case counts where the protocol's specification says it does. A technology of [xmip-core-route](https://github.com/IlleNilsson/xmip-core-route).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
