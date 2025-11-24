use geoengine_datatypes::raster::GridShapeAccess;
use hello_world::gdal_dataset_service_client::GdalDatasetServiceClient;
use hello_world::{GdalDatasetParameters};

pub mod hello_world {
    tonic::include_proto!("gdal_dataset_service");
}

const FILE_NAMES: [&str; 2] = [
    "../test_data/raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF",
    "../test_data/raster/modis_ndvi/flipped_axis_y/MOD13A2_M_NDVI_2014-01-01_flipped_y.tiff",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GdalDatasetServiceClient::connect("http://[::1]:50051").await?;

    loop {
        println!("Select a file: ");
        for (i, x) in FILE_NAMES.iter().enumerate() {
            println!("{}: {}", i, x);
        }

        let mut input_text = String::new();
        std::io::stdin().read_line(&mut input_text)?;
        let selection: usize = input_text.trim().parse().unwrap_or(0);
        if !validate_selection(selection) {
            continue;
        }
        let file_path = FILE_NAMES[selection];

        let request = tonic::Request::new(GdalDatasetParameters {
            file_path: file_path.to_string(),
            rasterband_channel: 1,
            width: 3600,
            height: 1800,
            file_not_found_handling: 0,
            allow_alphaband_as_mask: false,
        });

        let result = match client.load_tile_data(request).await {
            Err(e) => {
                println!("Error during request: {}", e);
                continue;
            }
            Ok(result) => result.into_inner()
        };

        println!("Received {} bytes of IPC data", result.data_size);

        // let grid = arrow_ipc_file_to_raster_tile_2d::<f64>(result.ipc_data, None);
        // match grid {
        //     Err(e) => {
        //         println!("Error converting response to raster tile: {}", e);
        //         continue;
        //     }
        //     Ok(grid) => {
        //         println!("Shape: {:?}", grid.grid_shape());
        //         println!("{:?}", grid);  
        //     }
        // }

        // println!("RESPONSE={:?}", result);
    }
}

fn validate_selection(selection: usize) -> bool {
    if selection >= FILE_NAMES.len() {
        println!("Invalid selection, try again.");
        return false;
    }
    true
}
