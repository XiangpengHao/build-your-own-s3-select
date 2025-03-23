This is the source code of my blog: [Build your own S3-Select in 400 lines of Rust](https://blog.xiangpeng.systems/posts/build-s3-select/).

### Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Run the server 

```bash
cargo run --bin server
```

### Run the client

```bash
cargo run --bin client
```

You should see the following output:

```bash
SQL send to storage: 
SELECT "aws-edge-locations"."city" FROM "aws-edge-locations" WHERE ("aws-edge-locations"."country" = 'United States')
+----------------+
| city           |
+----------------+
| New York       |
| Newark         |
| Miami          |
| Philadelphia   |
| Denver         |
| Houston        |
...
```











