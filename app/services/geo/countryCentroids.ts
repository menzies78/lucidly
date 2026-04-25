// Country centroids [lat, lng]. Used as a fallback for the geocoder when a
// city lookup fails, and by the legacy globe component while it still exists.
// Values are approximate population-weighted centroids — good enough for a
// "drop a marker on this country" use case.
export const COUNTRY_CENTROIDS: Record<string, [number, number]> = {
  GB: [54, -2], IE: [53.4, -7.6], US: [39, -98], CA: [56, -106],
  AU: [-25, 134], NZ: [-41, 174], DE: [51, 10], FR: [46, 2],
  ES: [40, -4], IT: [42, 12], NL: [52, 5], BE: [50.8, 4],
  CH: [47, 8], AT: [47.5, 14], SE: [62, 15], NO: [62, 10],
  DK: [56, 10], FI: [64, 26], PL: [52, 20], CZ: [50, 15],
  PT: [39.5, -8], GR: [39, 22], HU: [47, 19], RO: [46, 25],
  BG: [43, 25], HR: [45.2, 15.5], SK: [48.7, 19.5], SI: [46, 15],
  LT: [55.5, 24], LV: [57, 25], EE: [59, 25], RS: [44, 21],
  BA: [44, 18], AL: [41, 20], MK: [41.5, 22], ME: [42.5, 19.3],
  IS: [65, -18], LU: [49.8, 6.1], MT: [35.9, 14.4], CY: [35, 33],
  AE: [24, 54], SA: [24, 45], KW: [29.5, 47.5], BH: [26, 50.5],
  QA: [25.3, 51.2], OM: [21, 57], JO: [31, 36], LB: [33.8, 35.8],
  IL: [31.5, 35], TR: [39, 35], EG: [27, 30], MA: [32, -5],
  ZA: [-30, 25], KE: [-1, 38], NG: [10, 8], GH: [8, -2],
  TZ: [-6, 35], ET: [9, 38], DZ: [28, 3], TN: [34, 9],
  JP: [36, 138], KR: [36, 128], CN: [35, 105], HK: [22.3, 114.2],
  SG: [1.3, 103.8], MY: [4, 102], TH: [15, 101], ID: [-2, 118],
  PH: [13, 122], IN: [21, 78], PK: [30, 70], BD: [24, 90],
  VN: [16, 108], TW: [23.5, 121], MM: [19, 96], KH: [13, 105],
  BR: [-10, -55], AR: [-34, -64], MX: [23, -102], CO: [4, -72],
  CL: [-35, -71], PE: [-10, -76], VE: [8, -66], EC: [-2, -78],
  UY: [-33, -56], PY: [-23, -58], BO: [-17, -65],
  RU: [60, 100], UA: [49, 32], KZ: [48, 67], UZ: [41, 65],
  JE: [49.2, -2.1], GG: [49.5, -2.5], IM: [54.2, -4.5],
  MU: [-20.3, 57.6], LK: [7, 81], NP: [28, 84],
  PA: [9, -80], CR: [10, -84], DO: [19, -70], JM: [18, -77],
};
