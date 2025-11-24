use geoengine_datatypes::primitives::TimeInterval;
use geoengine_datatypes::raster::TileInformation;
use geoengine_datatypes::raster::raster_tile_2d_to_arrow_ipc_file;
use geoengine_datatypes::util::ByteSize;
use geoengine_datatypes::util::test::TestDefault;
use tonic::{Request, Response, Status, transport::Server};

use crate::hello_world::GdalDatasetParameters;
use crate::hello_world::gdal_dataset_service_server::GdalDatasetService;
use crate::hello_world::gdal_dataset_service_server::GdalDatasetServiceServer;

use geoengine_operators::source as operators;

pub mod hello_world {
    tonic::include_proto!("gdal_dataset_service");
}

#[derive(Debug, Default)]
pub struct TileReaderImpl {}

impl Into<geoengine_operators::source::GdalDatasetParameters> for GdalDatasetParameters {
    fn into(self) -> geoengine_operators::source::GdalDatasetParameters {
        let GdalDatasetParameters {
            file_path,
            rasterband_channel,
            width,
            height,
            file_not_found_handling,
            allow_alphaband_as_mask,
        } = self;

        let fnfh = if 0 == file_not_found_handling {
            geoengine_operators::source::FileNotFoundHandling::NoData
        } else {
            geoengine_operators::source::FileNotFoundHandling::Error
        };

        geoengine_operators::source::GdalDatasetParameters {
            file_path: file_path.into(),
            rasterband_channel: rasterband_channel as usize,
            width: width as usize,
            height: height as usize,
            file_not_found_handling: fnfh,
            no_data_value: None,
            allow_alphaband_as_mask,
            gdal_config_options: None,
            gdal_open_options: None,
            properties_mapping: None,
            retry: None,
            geo_transform: geoengine_operators::source::GdalDatasetGeoTransform::test_default(), // TODO (low): make this not constant / default
        }
    }
}

#[tonic::async_trait]
impl GdalDatasetService for TileReaderImpl {
    async fn load_tile_data(
        &self,
        request: Request<hello_world::GdalDatasetParameters>,
    ) -> Result<Response<hello_world::TileDataReply>, Status> {
        /* FILE: gdal_source/mod.rs line 492
        =====================================
        dataset_params: &GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
        cache_hint: CacheHint, */

        let params: operators::GdalDatasetParameters = request.into_inner().into();

        println!();
        println!("\t[INFO] Received a request with params:\n\t{:?}", params);
        println!();

        let tile_info = TileInformation {
            // TODO (low): copy-paste from xxxx/gdal_source/mod.rs line 2338
            tile_size_in_pixels: [100, 100].into(),
            global_tile_position: [0, 0].into(),
            global_geo_transform: TestDefault::test_default(),
        };
        let tile_time = TimeInterval::default(); // TODO (low): make this not constant / default
        let cache_hint = geoengine_datatypes::primitives::CacheHint::default();

        let result =
            // NOTE: this is a wrapper around the actual load_tile_* functions,
            // as those are private. This is only for testing.
            // also note that for this example I've opened 
            operators::gdal_source::load_tile::<u8>(&params, tile_info, tile_time, cache_hint);

        match result {
            Err(_) => Ok(Response::new(hello_world::TileDataReply {
                success: false,
                data_size: 0,
                // ipc_data: vec![],
            })),
            Ok(ok) => {
                use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
                let bytes = ok.byte_size();
                Ok(Response::new(hello_world::TileDataReply {
                    success: true,
                    data_size: bytes as u32,
                    // ipc_data: raster_tile_2d_to_arrow_ipc_file(
                    //     ok,
                    //     SpatialReferenceOption::Unreferenced,
                    // ).expect("it should be possible to convert the tile to arrow ipc"),
                }))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Server listening on [::1]:50051");
    let addr = "[::1]:50051".parse()?;
    let greeter = TileReaderImpl::default();

    Server::builder()
        .add_service(GdalDatasetServiceServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}
