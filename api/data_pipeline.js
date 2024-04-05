const fetch = require('node-fetch');

module.exports = async (req, res) => {

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_KEY = process.env.SUPABASE_KEY;
  const APIFY_API_URL = process.env.APIFY_API_URL;
  
// Function to transform data from scraper to match Supabase columns
const transformDataForSupabase = (rawData) => rawData.flat().map((item) => ({
    zpid: item.zpid,
    _type: item.propertyType ?? null,
    latitude: item.location?.latitude ?? null,
    longitude: item.location?.longitude ?? null,
    street_address: item.address?.streetAddress ?? '',
    zipcode: item.address?.zipcode ?? '',
    city: item.address?.city ?? '',
    state: item.address?.state ?? '',
    media: item.media ? JSON.stringify(item.media.propertyPhotoLinks) : '{}',
    currency: item.currency ?? 'USD',
    country: item.country ?? '',
    listing_datetime_on_zillow: item.listingDateTimeOnZillow ?? null,
    best_guess_time_zone: item.bestGuessTimeZone ?? '',
    last_sold_date: item.lastSoldDate ?? null,
    bathrooms: item.bathrooms ?? null,
    bedrooms: item.bedrooms ?? null,
    living_area: item.livingArea ?? null,
    year_built: item.yearBuilt ?? null,
    lot_size: item.lotSizeWithUnit ? (item.lotSizeWithUnit.lotSizeUnit === 'acres' ? Math.round(item.lotSizeWithUnit.lotSize * 43560) : Math.round(item.lotSizeWithUnit.lotSize)) : null,
    lot_size_unit: 'squareFeet', // Assuming conversion to square feet
    property_type: item.propertyType ?? '',
    listing_status: item.listing?.listingStatus ?? '',
    provider_listing_id: item.listing?.providerListingID ?? '',
    days_on_zillow: item.daysOnZillow ?? null,
    price_value: item.price?.value ?? null,
    price_per_square_foot: item.price?.pricePerSquareFoot ?? null,
    zestimate: item.estimates?.zestimate ?? null,
    rent_zestimate: item.estimates?.rentZestimate ?? null,
    tax_assessment_value: item.taxAssessment?.value ?? null,
    tax_assessment_year: item.taxAssessment?.year ?? null,
    ssid: item.ssid ?? null,
    listing_data_source: item.listingDataSource ?? '',
    home_status: item.homeStatus ?? '',
    region_string: item.regionString ?? '',
    url: item.url ?? '',
    zestimate_low_percent: item.zestimateLowPercent ?? '',
    zestimate_high_percent: item.zestimateHighPercent ?? '',
    restimate_low_percent: item.restimateLowPercent ?? '',
    restimate_high_percent: item.restimateHighPercent ?? '',
    parent_region_name: item.parentRegion?.name ?? '',
    county_fips: item.countyFIPS ?? '',
    parcel_id: item.parcelId ?? '',
    page_view_count: item.pageViewCount ?? 0,
    favorite_count: item.favoriteCount ?? 0,
    total_monthly_cost: item.affordabilityEstimate?.totalMonthlyCost ?? 0,
    brokerage_name: item.brokerageName ?? '',
    fifteen_year_fixed_rate: item.mortgageRates?.fifteenYearFixedRate ?? 0,
    thirty_year_fixed_rate: item.mortgageRates?.thirtyYearFixedRate ?? 0,
    arm5_rate: item.mortgageRates?.arm5Rate ?? 0,
    property_tax_rate: item.propertyTaxRate ?? 0,
    mlsid: item.mlsid ?? '',
    virtual_tour_url: item.virtualTourUrl ?? '',
    photo_count: item.photoCount ?? 0,
    living_area_units: item.livingAreaUnits ?? '',
    posting_product_type: item.postingProductType ?? '',
    city_id: item.cityId ?? 0,
    state_id: item.stateId ?? 0,
    zip_plus_four: item.zipPlusFour ?? '',
    date_posted_string: item.datePostedString ?? '',
    posting_url: item.postingUrl ?? '',
    last_sold_price: item.lastSoldPrice ?? 0,
    county: item.county ?? '',
    rental_applications_accepted_type: item.rentalApplicationsAcceptedType ?? '',
    attribution_info: JSON.stringify(item.attributionInfo ?? {}),
    photos: JSON.stringify(item.photos ?? []),
}));

// Batch insert function

const batchInsertIntoSupabase = async (data) => {
    try {
      const response = await fetch(SUPABASE_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'apikey': SUPABASE_KEY,
          'Authorization': `Bearer ${SUPABASE_KEY}`,
        },
        body: JSON.stringify(data),
      });
  
      console.log(`Response Status: ${response.status}`);
      // Improved error handling: Safely attempt to parse JSON first
      const resultText = await response.text();
      
      // Handle non-OK responses with detailed error message
      if (!response.ok) {
        const errorDetails = resultText ? ` Response Body: ${resultText}` : '';
        throw new Error(`HTTP error! Status: ${response.status}${errorDetails}`);
      }
  
      // Safely parse JSON, accounting for potential empty responses
      const result = resultText ? JSON.parse(resultText) : {};
      console.log('Batch insert successful:', result);
    } catch (error) {
      console.error('Error in batch insert to Supabase:', error);
    }
  };

// Function to fetch data from Apify API
const fetchDataFromApify = async () => {
  try {
      const response = await fetch(APIFY_API_URL);
      if (!response.ok) {
          throw new Error(`Failed to fetch data from Apify: ${response.statusText}`);
      }
      const data = await response.json();
      // Assuming the data structure is an array of items
      return data; // Adjust this based on the actual structure of your data
  } catch (error) {
      console.error('Error fetching data from Apify:', error);
      return null; // Return null or an empty array as a fallback
  }
};

// Main function to run the process
const runProcess = async () => {
  const rawData = await fetchDataFromApify();
  if (!rawData) {
      console.error('No data fetched from Apify.');
      return;
  }
  const transformedData = transformDataForSupabase(rawData);
  await batchInsertIntoSupabase(transformedData);
  console.log('Execution completed.');
};

runProcess();

res.json({ message: "This is the response from your Vercel serverless function!" });
};