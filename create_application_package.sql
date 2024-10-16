USE ROLE ACCOUNTADMIN;

DROP APPLICATION PACKAGE IF EXISTS its_nativeapps_snoptimizer_v1;
CREATE APPLICATION PACKAGE its_nativeapps_snoptimizer_v1;


ALTER APPLICATION PACKAGE its_nativeapps_snoptimizer_v1
  ADD VERSION v1
  USING '@ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE'
  LABEL = 'Consumer Version 1.0';

ALTER APPLICATION PACKAGE its_nativeapps_snoptimizer_v1
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = v1
  PATCH = 0;

-- ALTER APPLICATION PACKAGE its_nativeapps_snoptimizer_v1
--   SET DISTRIBUTION = EXTERNAL;