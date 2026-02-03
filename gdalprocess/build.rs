

fn main() {
    tonic_prost_build::compile_protos("./proto/test_impl.proto").unwrap();
    tonic_prost_build::compile_protos("./proto/simple_service.proto").unwrap();
}
