use crate::{error::Error, primitives::DateTimeError};
use arrow::error::ArrowError;
use snafu::prelude::*;

use super::TimeInstance;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[snafu(context(suffix(false)))] // disables default `Snafu` suffix
pub enum PrimitivesError {
    UnallowedEmpty,
    UnclosedPolygonRing,
    InvalidSpatialResolution {
        value: f64,
    },
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowError,
    },
    InvalidConversion,

    #[snafu(display("Time instance must be between {} and {}, but is {}", min.inner(), max.inner(), is))]
    InvalidTimeInstance {
        min: TimeInstance,
        max: TimeInstance,
        is: i64,
    },

    #[snafu(display("The datetime string {datetime} is not a RFC timestamp. DateTimeError: {source}"))]
    NoDateTimeParse {
        datetime: String,
        source: DateTimeError,
    },

    #[snafu(display("Expect RFC 3339 timestamp string or Unix timestamp integer"))]
    InvalidStringOrTimeStamp {
        // source: Box<dyn std::error::Error>, // TODO (mid): make this nice
    },

}

impl From<PrimitivesError> for Error {
    fn from(error: PrimitivesError) -> Self {
        Error::Primitives { source: error }
    }
}

impl From<ArrowError> for PrimitivesError {
    fn from(source: ArrowError) -> Self {
        PrimitivesError::ArrowInternal { source }
    }
}
