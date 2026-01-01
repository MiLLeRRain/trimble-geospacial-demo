namespace Trimble.Geospatial.Api.Models;

public enum BuildingCandidateOrderBy
{
    HeightDesc,
    HeightRangeDesc
}

public static class BuildingCandidateOrderByParser
{
    public static bool TryParse(string? value, out BuildingCandidateOrderBy orderBy)
    {
        orderBy = BuildingCandidateOrderBy.HeightDesc;

        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        return value switch
        {
            "heightDesc" => Set(BuildingCandidateOrderBy.HeightDesc, out orderBy),
            "heightRangeDesc" => Set(BuildingCandidateOrderBy.HeightRangeDesc, out orderBy),
            _ => false
        };
    }

    private static bool Set(BuildingCandidateOrderBy value, out BuildingCandidateOrderBy orderBy)
    {
        orderBy = value;
        return true;
    }
}
